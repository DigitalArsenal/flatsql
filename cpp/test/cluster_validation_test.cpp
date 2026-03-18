#include "flatsql/database.h"
#include "../schemas/test_schema_generated.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace flatsql;

namespace {

using Clock = std::chrono::steady_clock;

std::vector<uint8_t> createUserFlatBuffer(int32_t id, const std::string& name,
                                          const std::string& email, int32_t age) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUserDirect(builder, id, name.c_str(), email.c_str(), age);
    builder.Finish(user, "USER");
    const uint8_t* buf = builder.GetBufferPointer();
    size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

bool verifyUserFlatBuffer(const uint8_t* data, size_t length) {
    flatbuffers::Verifier verifier(data, length);
    return verifier.VerifyBuffer<test::User>("USER");
}

Value extractUserField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    auto user = test::GetUser(data);
    if (!user) return std::monostate{};

    if (fieldName == "id") return user->id();
    if (fieldName == "name") return user->name() ? std::string(user->name()->c_str(), user->name()->size()) : std::string();
    if (fieldName == "email") return user->email() ? std::string(user->email()->c_str(), user->email()->size()) : std::string();
    if (fieldName == "age") return user->age();
    return std::monostate{};
}

std::string expectedName(uint64_t id) {
    return "User" + std::to_string(id);
}

std::string expectedEmail(uint64_t id) {
    return "user" + std::to_string(id) + "@cluster.test";
}

int32_t expectedAge(uint64_t id) {
    return 20 + static_cast<int32_t>(id % 50);
}

int64_t valueToInt64(const Value& value) {
    return std::visit([](const auto& current) -> int64_t {
        using T = std::decay_t<decltype(current)>;
        if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                      std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                      std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                      std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>) {
            return static_cast<int64_t>(current);
        } else {
            throw std::runtime_error("expected integer-compatible Value");
        }
    }, value);
}

struct LatencyStats {
    uint64_t count = 0;
    double totalMicros = 0;
    double maxMicros = 0;
    std::vector<double> samples;

    void record(Clock::duration elapsed) {
        const double micros = std::chrono::duration<double, std::micro>(elapsed).count();
        count++;
        totalMicros += micros;
        maxMicros = std::max(maxMicros, micros);
        if ((count & 31u) == 0u) {
            samples.push_back(micros);
        }
    }
};

struct ScenarioReport {
    std::string name;
    size_t durationSeconds = 0;
    size_t readerCount = 0;
    uint64_t writes = 0;
    uint64_t reads = 0;
    uint64_t verifies = 0;
    uint64_t readerMisses = 0;
    uint64_t verifyFailures = 0;
    uint64_t readerStalls = 0;
    LatencyStats writeLatency;
    LatencyStats readLatency;
    LatencyStats verifyLatency;
    std::vector<std::string> errors;
};

struct ScenarioConfig {
    std::string name;
    size_t durationSeconds = 60;
    size_t readerCount = 8;
    size_t stallThresholdMs = 1000;
};

template <typename T>
T parseArgValue(const std::string& arg, const std::string& prefix, T defaultValue) {
    if (arg.rfind(prefix, 0) != 0) {
        return defaultValue;
    }
    return static_cast<T>(std::stoll(arg.substr(prefix.size())));
}

double percentile95(std::vector<double> values) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(std::ceil((values.size() - 1) * 0.95));
    return values[std::min(index, values.size() - 1)];
}

void mergeLatency(const std::vector<LatencyStats>& inputs, LatencyStats& output) {
    for (const auto& input : inputs) {
        output.count += input.count;
        output.totalMicros += input.totalMicros;
        output.maxMicros = std::max(output.maxMicros, input.maxMicros);
        output.samples.insert(output.samples.end(), input.samples.begin(), input.samples.end());
    }
}

void printLatency(const std::string& label, const LatencyStats& stats) {
    const double average = stats.count > 0 ? stats.totalMicros / static_cast<double>(stats.count) : 0.0;
    const double p95 = percentile95(stats.samples);
    std::cout
        << "  " << label
        << " avg=" << average / 1000.0 << "ms"
        << " p95=" << p95 / 1000.0 << "ms"
        << " max=" << stats.maxMicros / 1000.0 << "ms"
        << " count=" << stats.count
        << std::endl;
}

ScenarioReport runScenario(const ScenarioConfig& config) {
    const std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";
    const DatabaseSchema parsedSchema = SchemaParser::parse(schema, config.name);

    auto sharedStore = std::make_shared<StreamingFlatBufferStore>();
    auto accessMutex = std::make_shared<std::shared_mutex>();
    const auto sqlitePath = std::filesystem::temp_directory_path() /
        ("flatsql-" + config.name + "-" + std::to_string(::getpid()) + ".sqlite");
    std::filesystem::remove(sqlitePath);
    std::filesystem::remove(sqlitePath.string() + "-wal");
    std::filesystem::remove(sqlitePath.string() + "-shm");

    FlatSQLDatabase::RuntimeOptions options;
    options.sharedStore = sharedStore;
    options.accessMutex = accessMutex;
    options.sqlite.path = sqlitePath.string();
    options.sqlite.enableWal = true;
    options.sqlite.busyTimeoutMs = 250;
    options.sqlite.maxBusyRetries = 8;
    options.sqlite.busyBackoffMs = 1;

    auto writerDb = FlatSQLDatabase(parsedSchema, options);
    writerDb.registerFileId("USER", "User");
    writerDb.setFieldExtractor("User", extractUserField);

    std::vector<std::unique_ptr<FlatSQLDatabase>> readerDbs;
    readerDbs.reserve(config.readerCount);
    for (size_t i = 0; i < config.readerCount; i++) {
        auto db = std::make_unique<FlatSQLDatabase>(
            parsedSchema,
            options
        );
        db->registerFileId("USER", "User");
        db->setFieldExtractor("User", extractUserField);
        readerDbs.push_back(std::move(db));
    }

    auto verifierDb = FlatSQLDatabase(parsedSchema, options);
    verifierDb.registerFileId("USER", "User");
    verifierDb.setFieldExtractor("User", extractUserField);

    std::atomic<bool> running{true};
    std::atomic<uint64_t> maxCommittedId{0};
    std::atomic<uint64_t> writes{0};
    std::atomic<uint64_t> reads{0};
    std::atomic<uint64_t> verifies{0};
    std::atomic<uint64_t> readerMisses{0};
    std::atomic<uint64_t> verifyFailures{0};
    std::atomic<uint64_t> readerStalls{0};
    std::mutex errorMutex;
    std::vector<std::string> errors;

    auto recordError = [&](const std::string& error) {
        std::lock_guard lock(errorMutex);
        errors.push_back(error);
        running.store(false);
    };

    const auto endTime = Clock::now() + std::chrono::seconds(config.durationSeconds);
    std::vector<LatencyStats> readerLatencies(config.readerCount);
    LatencyStats writerLatency;
    LatencyStats verifierLatency;
    std::vector<std::atomic<int64_t>> readerProgress(config.readerCount);
    const auto startMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now().time_since_epoch()
    ).count();
    for (auto& progress : readerProgress) {
        progress.store(startMillis);
    }

    std::thread writer([&]() {
        uint64_t nextId = 1;
        try {
            while (running.load() && Clock::now() < endTime) {
                const auto started = Clock::now();
                const auto flatbuffer = createUserFlatBuffer(
                    static_cast<int32_t>(nextId),
                    expectedName(nextId),
                    expectedEmail(nextId),
                    expectedAge(nextId)
                );
                writerDb.ingestOne(flatbuffer.data(), flatbuffer.size());
                writerLatency.record(Clock::now() - started);
                maxCommittedId.store(nextId, std::memory_order_release);
                writes.fetch_add(1, std::memory_order_relaxed);
                if ((nextId & 127u) == 0u) {
                    std::this_thread::yield();
                }
                nextId++;
            }
        } catch (const std::exception& e) {
            recordError(std::string("writer: ") + e.what());
        }
    });

    std::vector<std::thread> readers;
    readers.reserve(config.readerCount);
    for (size_t i = 0; i < config.readerCount; i++) {
        readers.emplace_back([&, i]() {
            std::mt19937_64 rng(0xC1A57ULL + i);
            try {
                while (running.load() && Clock::now() < endTime) {
                    const uint64_t committed = maxCommittedId.load(std::memory_order_acquire);
                    if (committed == 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        continue;
                    }

                    std::uniform_int_distribution<uint64_t> pick(1, committed);
                    const uint64_t targetId = pick(rng);
                    const auto started = Clock::now();
                    auto result = readerDbs[i]->query(
                        "SELECT * FROM User WHERE id = ?",
                        std::vector<Value>{static_cast<int64_t>(targetId)}
                    );
                    readerLatencies[i].record(Clock::now() - started);
                    reads.fetch_add(1, std::memory_order_relaxed);

                    if (result.rowCount() != 1) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        result = readerDbs[i]->query(
                            "SELECT * FROM User WHERE id = ?",
                            std::vector<Value>{static_cast<int64_t>(targetId)}
                        );
                    }

                    if (result.rowCount() != 1) {
                        readerMisses.fetch_add(1, std::memory_order_relaxed);
                        recordError("reader miss for id " + std::to_string(targetId));
                        return;
                    }

                    const auto& row = result.rows[0];
                    const auto id = valueToInt64(row[0]);
                    const auto name = std::get<std::string>(row[1]);
                    const auto email = std::get<std::string>(row[2]);
                    const auto age = valueToInt64(row[3]);
                    if (id != static_cast<int64_t>(targetId) ||
                        name != expectedName(targetId) ||
                        email != expectedEmail(targetId) ||
                        age != expectedAge(targetId)) {
                        readerMisses.fetch_add(1, std::memory_order_relaxed);
                        recordError("reader value mismatch for id " + std::to_string(targetId));
                        return;
                    }

                    const auto nowMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                        Clock::now().time_since_epoch()
                    ).count();
                    readerProgress[i].store(nowMillis, std::memory_order_relaxed);
                }
            } catch (const std::exception& e) {
                recordError(std::string("reader ") + std::to_string(i) + ": " + e.what());
            }
        });
    }

    std::thread verifier([&]() {
        std::mt19937_64 rng(0xBEEFULL);
        try {
            while (running.load() && Clock::now() < endTime) {
                const uint64_t committed = maxCommittedId.load(std::memory_order_acquire);
                if (committed == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                    continue;
                }

                std::uniform_int_distribution<uint64_t> pick(1, committed);
                const uint64_t targetId = pick(rng);
                const auto started = Clock::now();
                auto result = verifierDb.query(
                    "SELECT _data FROM User WHERE id = ?",
                    std::vector<Value>{static_cast<int64_t>(targetId)}
                );
                if (result.rowCount() != 1) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    result = verifierDb.query(
                        "SELECT _data FROM User WHERE id = ?",
                        std::vector<Value>{static_cast<int64_t>(targetId)}
                    );
                }
                verifierLatency.record(Clock::now() - started);
                verifies.fetch_add(1, std::memory_order_relaxed);

                if (result.rowCount() != 1 || !std::holds_alternative<std::vector<uint8_t>>(result.rows[0][0])) {
                    verifyFailures.fetch_add(1, std::memory_order_relaxed);
                    recordError(
                        "verifier lookup failed for id " + std::to_string(targetId) +
                        " rowCount=" + std::to_string(result.rowCount())
                    );
                    return;
                }

                const auto& blob = std::get<std::vector<uint8_t>>(result.rows[0][0]);
                if (blob.empty() || !verifyUserFlatBuffer(blob.data(), blob.size())) {
                    verifyFailures.fetch_add(1, std::memory_order_relaxed);
                    recordError(
                        "verifier blob invalid for id " + std::to_string(targetId) +
                        " size=" + std::to_string(blob.size())
                    );
                    return;
                }

                const auto* user = test::GetUser(blob.data());
                if (!user || user->id() != static_cast<int32_t>(targetId)) {
                    verifyFailures.fetch_add(1, std::memory_order_relaxed);
                    recordError("verifier value mismatch for id " + std::to_string(targetId));
                    return;
                }
            }
        } catch (const std::exception& e) {
            recordError(std::string("verifier: ") + e.what());
        }
    });

    std::thread monitor([&]() {
        while (running.load() && Clock::now() < endTime) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (maxCommittedId.load(std::memory_order_acquire) == 0) {
                continue;
            }
            const auto nowMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                Clock::now().time_since_epoch()
            ).count();
            for (auto& progress : readerProgress) {
                const auto idleMs = nowMillis - progress.load(std::memory_order_relaxed);
                if (idleMs > static_cast<int64_t>(config.stallThresholdMs)) {
                    readerStalls.fetch_add(1, std::memory_order_relaxed);
                    progress.store(nowMillis, std::memory_order_relaxed);
                }
            }
        }
    });

    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }
    verifier.join();
    monitor.join();
    running.store(false);

    ScenarioReport report;
    report.name = config.name;
    report.durationSeconds = config.durationSeconds;
    report.readerCount = config.readerCount;
    report.writes = writes.load();
    report.reads = reads.load();
    report.verifies = verifies.load();
    report.readerMisses = readerMisses.load();
    report.verifyFailures = verifyFailures.load();
    report.readerStalls = readerStalls.load();
    report.writeLatency = writerLatency;
    report.verifyLatency = verifierLatency;
    mergeLatency(readerLatencies, report.readLatency);
    report.errors = std::move(errors);

    std::filesystem::remove(sqlitePath);
    std::filesystem::remove(sqlitePath.string() + "-wal");
    std::filesystem::remove(sqlitePath.string() + "-shm");

    return report;
}

void printScenarioReport(const ScenarioReport& report) {
    std::cout << "Scenario: " << report.name
              << " duration=" << report.durationSeconds << "s"
              << " readers=" << report.readerCount << std::endl;
    std::cout << "  throughput writes=" << (report.writes / static_cast<double>(report.durationSeconds))
              << "/s reads=" << (report.reads / static_cast<double>(report.durationSeconds))
              << "/s verifies=" << (report.verifies / static_cast<double>(report.durationSeconds))
              << "/s" << std::endl;
    printLatency("write", report.writeLatency);
    printLatency("read", report.readLatency);
    printLatency("verify", report.verifyLatency);
    std::cout << "  failures reader_miss=" << report.readerMisses
              << " verify=" << report.verifyFailures
              << " stalls=" << report.readerStalls
              << std::endl;
    for (const auto& error : report.errors) {
        std::cout << "  error: " << error << std::endl;
    }
}

}  // namespace

int main(int argc, char** argv) {
    ScenarioConfig base;
    base.name = "base";

    ScenarioConfig stretch;
    stretch.name = "stretch";
    stretch.durationSeconds = 30;
    stretch.readerCount = 12;

    for (int i = 1; i < argc; i++) {
        const std::string arg = argv[i];
        base.durationSeconds = parseArgValue<size_t>(arg, "--duration=", base.durationSeconds);
        base.readerCount = parseArgValue<size_t>(arg, "--readers=", base.readerCount);
        stretch.durationSeconds = parseArgValue<size_t>(arg, "--stretch-duration=", stretch.durationSeconds);
        stretch.readerCount = parseArgValue<size_t>(arg, "--stretch-readers=", stretch.readerCount);
    }

    const auto baseReport = runScenario(base);
    printScenarioReport(baseReport);
    const auto stretchReport = runScenario(stretch);
    printScenarioReport(stretchReport);

    const bool failed =
        baseReport.readerMisses > 0 ||
        baseReport.verifyFailures > 0 ||
        baseReport.readerStalls > 0 ||
        !baseReport.errors.empty() ||
        stretchReport.readerMisses > 0 ||
        stretchReport.verifyFailures > 0 ||
        stretchReport.readerStalls > 0 ||
        !stretchReport.errors.empty();

    if (failed) {
        std::cerr << "Cluster validation failed" << std::endl;
        return 1;
    }

    std::cout << "Cluster validation passed" << std::endl;
    return 0;
}
