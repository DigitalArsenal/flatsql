import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const repositoriesDirectory = path.resolve(testDirectory, "../../../../..");
const standardsDirectory = path.join(
  repositoriesDirectory,
  "main-packages/spacedatastandards.org",
);
const flatcPath = path.join(
  repositoriesDirectory,
  "main-packages/flatbuffers/build/flatc",
);

const boundarySchemas = Object.freeze([
  {
    code: "FSO",
    requiredTokens: [
      "enum flatSqlNodeOperation",
      "enum flatSqlNodeStatus",
      "table FTB",
      "SCHEMA_IDL:[ubyte]",
      "TABLE_BINDINGS:[FTB]",
      "QUERY:[ubyte]",
      "PARAMETER_COUNT:uint",
      "RETENTION_MAX_RECORDS:ulong",
      "RETENTION_MAX_AGE_MILLIS:ulong",
      "COMPACTION_TARGET_BYTES:ulong",
    ],
  },
  {
    code: "FSB",
    requiredTokens: [
      "enum flatSqlByteStreamKind",
      "REQUEST_ID:ulong",
      "CHUNK_SEQUENCE:uint",
      "FINAL:bool",
      "TOTAL_BYTES:ulong",
      "DATA:[ubyte]",
      "SHA256:[ubyte]",
    ],
  },
]);

async function readSchema(code) {
  const schemaPath = path.join(standardsDirectory, `schema/${code}/main.fbs`);
  try {
    return {
      schemaPath,
      source: await readFile(schemaPath, "utf8"),
    };
  } catch (error) {
    if (error?.code === "ENOENT") {
      assert.fail(`canonical SDS ${code} schema is missing: ${schemaPath}`);
    }
    throw error;
  }
}

test("FlatSQL node boundaries are canonical SDS schemas with aligned layouts", async () => {
  const outputRoot = await mkdtemp(path.join(tmpdir(), "flatsql-sds-boundary-"));
  try {
    for (const boundary of boundarySchemas) {
      const { schemaPath, source } = await readSchema(boundary.code);
      assert.match(source, new RegExp(`root_type\\s+${boundary.code}\\s*;`));
      assert.match(
        source,
        new RegExp(`file_identifier\\s+"\\$${boundary.code}"\\s*;`),
      );
      for (const token of boundary.requiredTokens) {
        assert.ok(
          source.includes(token),
          `${boundary.code} is missing required contract token ${token}`,
        );
      }

      const outputDirectory = path.join(outputRoot, boundary.code);
      await execFileAsync(flatcPath, [
        "--no-warnings",
        "--cpp",
        "--aligned",
        "-o",
        outputDirectory,
        schemaPath,
      ]);
      const alignedHeader = await readFile(
        path.join(outputDirectory, "main_aligned.h"),
        "utf8",
      );
      assert.match(alignedHeader, new RegExp(`struct\\s+${boundary.code}`));
      assert.match(
        alignedHeader,
        new RegExp(`constexpr\\s+size_t\\s+${boundary.code}_SIZE\\s*=`),
      );
      assert.match(
        alignedHeader,
        new RegExp(`constexpr\\s+size_t\\s+${boundary.code}_ALIGN\\s*=`),
      );
    }

    const recSource = await readFile(
      path.join(standardsDirectory, "schema/REC/main.fbs"),
      "utf8",
    );
    assert.match(recSource, /include\s+"\.\.\/FSO\/main\.fbs"\s*;/);
    assert.match(recSource, /include\s+"\.\.\/FSB\/main\.fbs"\s*;/);
    const recordUnion = recSource.match(/union\s+RecordType\s*\{([\s\S]*?)\}/)?.[1];
    assert.ok(recordUnion?.match(/\bFSO\b/), "REC must register FSO");
    assert.ok(recordUnion?.match(/\bFSB\b/), "REC must register FSB");
  } finally {
    await rm(outputRoot, { recursive: true, force: true });
  }
});
