#ifndef FLATSQL_P4_PAGE_CACHE_H
#define FLATSQL_P4_PAGE_CACHE_H
#include <algorithm>
#include <iterator>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace flatsql { namespace p4 {
// Shared admission budget for immutable pages and verified segment metadata.
// Reservations include loads in progress. An external shared_ptr pins an entry;
// admission fails when pins exhaust the budget. No cache lock covers I/O.
class PageCache {
    using Clock=std::chrono::steady_clock;
    struct Flight {
        bool done=false;
        int32_t status=0;
        std::shared_ptr<const void> value;
    };
    struct Entry {
        std::shared_ptr<const void> value;
        size_t bytes=0;
        bool loading=true;
        std::shared_ptr<Flight> flight;
        Clock::time_point touched=Clock::now();
        std::list<std::string>::iterator lru;
    };
    std::mutex mu_;
    std::condition_variable ready_;
    std::map<std::string,Entry> entries_;
    std::list<std::string> lru_;
    size_t limit_=32u<<20,used_=0,peak_=0;
    size_t coalesced_=0;
    using Garbage=std::vector<std::shared_ptr<const void>>;
    void erase(std::map<std::string,Entry>::iterator it,Garbage& garbage) {
        garbage.push_back(std::move(it->second.value));
        used_-=it->second.bytes; lru_.erase(it->second.lru); entries_.erase(it);
    }
public:
    void configure(size_t bytes) { std::lock_guard<std::mutex> g(mu_); limit_=bytes; }
    size_t used() { std::lock_guard<std::mutex> g(mu_); return used_; }
    size_t peak() { std::lock_guard<std::mutex> g(mu_); return peak_; }
    size_t coalesced() { std::lock_guard<std::mutex> g(mu_); return coalesced_; }
    // Evict idle unpinned entries; zero age supports explicit pressure trimming.
    void trim(std::chrono::seconds age=std::chrono::seconds(30)) {
        Garbage garbage;
        std::lock_guard<std::mutex> g(mu_); const auto now=Clock::now();
        size_t remaining=lru_.size();
        while(remaining-- && !lru_.empty()) {
            auto cur=entries_.find(lru_.back());
            if(now-cur->second.touched<age) break;
            if(!cur->second.loading && cur->second.value.use_count()==1) erase(cur,garbage);
            else {
                // A live pin is reconsidered after another idle interval.
                cur->second.touched=now;
                lru_.splice(lru_.begin(),lru_,cur->second.lru);
            }
        }
    }
    void dropPrefix(const std::string& prefix) {
        Garbage garbage;
        std::lock_guard<std::mutex> g(mu_);
        for(auto it=entries_.lower_bound(prefix);it!=entries_.end() && it->first.compare(0,prefix.size(),prefix)==0;) {
            auto cur=it++;
            if(!cur->second.loading && cur->second.value.use_count()==1) erase(cur,garbage);
        }
    }
    template<class T> std::shared_ptr<const T> find(const std::string& key) {
        std::lock_guard<std::mutex> g(mu_);
        auto it=entries_.find(key);
        if(it==entries_.end() || it->second.loading) return {};
        it->second.touched=Clock::now(); lru_.splice(lru_.begin(),lru_,it->second.lru);
        return std::static_pointer_cast<const T>(it->second.value);
    }
    template<class T,class Loader> std::shared_ptr<const T> load(const std::string& key,size_t bytes,Loader loader,size_t workspace=0,int32_t* completionStatus=nullptr) {
        Garbage garbage;
        std::unique_lock<std::mutex> g(mu_);
        for(;;) {
            auto it=entries_.find(key);
            if(it==entries_.end()) break;
            if(it->second.loading) {
                ++coalesced_;
                const auto flight=it->second.flight;
                ready_.wait(g,[&] { return flight->done; });
                if(completionStatus) *completionStatus=flight->status;
                return std::static_pointer_cast<const T>(flight->value);
            }
            it->second.touched=Clock::now(); lru_.splice(lru_.begin(),lru_,it->second.lru);
            if(completionStatus) *completionStatus=0;
            return std::static_pointer_cast<const T>(it->second.value);
        }
        // Account conservatively for map/list nodes, keys and shared ownership.
        const size_t overhead=512+2*key.capacity();
        if(bytes>limit_ || overhead>limit_-bytes || workspace>limit_-bytes-overhead) return {};
        bytes+=overhead+workspace;
        for(auto it=lru_.end();used_>limit_-bytes && it!=lru_.begin();) {
            --it; auto entry=entries_.find(*it);
            if(!entry->second.loading && entry->second.value.use_count()==1) {
                auto prev=it; const bool first=it==lru_.begin();
                if(!first) --prev;
                erase(entry,garbage);
                it=first?lru_.begin():std::next(prev);
            }
        }
        if(used_>limit_-bytes) return {};
        auto flight=std::make_shared<Flight>();
        lru_.push_front(key); Entry e; e.bytes=bytes; e.lru=lru_.begin(); e.flight=flight;
        entries_.emplace(key,std::move(e)); used_+=bytes; peak_=std::max(peak_,used_);
        g.unlock();
        garbage.clear();
        std::shared_ptr<const T> value;
#if defined(__cpp_exceptions)
        try { value=loader(); }
        catch(...) { g.lock(); flight->done=true; erase(entries_.find(key),garbage); ready_.notify_all(); throw; }
#else
        value=loader();
#endif
        g.lock();
        auto it=entries_.find(key);
        if(value && completionStatus) *completionStatus=0;
        flight->status=completionStatus?*completionStatus:0;
        flight->value=value; flight->done=true;
        if(value) { it->second.value=value; it->second.loading=false; it->second.flight.reset(); it->second.bytes-=workspace; used_-=workspace; }
        else erase(it,garbage);
        ready_.notify_all(); return value;
    }
};
} }
#endif
