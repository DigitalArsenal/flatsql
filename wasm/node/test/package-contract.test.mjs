import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const nodeDirectory = path.resolve(testDirectory, "..");
const repositoriesDirectory = path.resolve(testDirectory, "../../../../..");
const sdkDirectory = path.join(
  repositoriesDirectory,
  "ancillary-packages/space-data-module-sdk",
);
const standardsDirectory = path.join(
  repositoriesDirectory,
  "main-packages/spacedatastandards.org",
);

const manifestPath = path.join(nodeDirectory, "plugin-manifest.json");
const artifactPath = path.join(
  nodeDirectory,
  "dist/isomorphic/module.wasm",
);
const artifactMetadataPath = path.join(
  nodeDirectory,
  "dist/isomorphic/artifact.json",
);
const publisherPath = path.join(nodeDirectory, "publisher.json");

const requiredExports = Object.freeze([
  "plugin_alloc",
  "plugin_free",
  "plugin_get_manifest_flatbuffer",
  "plugin_get_manifest_flatbuffer_size",
  "plugin_invoke_stream",
]);

const productionExportAllowlist = Object.freeze([
  "__indirect_function_table",
  "_initialize",
  "memory",
  ...requiredExports,
].sort());

const requiredMethods = Object.freeze([
  "append_records",
  "query_records",
  "configure_index",
  "upsert_view",
  "compact",
  "configure_retention",
  "snapshot",
  "reload",
]);

const forbiddenHostLinks = Object.freeze([
  "storage_engine_link",
  "flatsql_link",
  "engineLinkage",
  "linkedStore",
  "hostcap",
]);

async function importSdk(relativePath) {
  return import(pathToFileURL(path.join(sdkDirectory, relativePath)).href);
}

async function readRequired(filePath, description) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error?.code === "ENOENT") {
      assert.fail(`${description} is missing: ${filePath}`);
    }
    throw error;
  }
}

async function loadBoundaryCatalog() {
  return Promise.all(
    ["FSO", "FSB"].map(async (schemaCode) => {
      const idl = await readFile(
        path.join(standardsDirectory, `schema/${schemaCode}/main.fbs`),
        "utf8",
      );
      return {
        schemaCode,
        schemaName: `${schemaCode}.fbs`,
        fileIdentifier: idl.match(/file_identifier\s+"([^"]+)"/)?.[1],
        rootTypeName: idl.match(/root_type\s+([A-Za-z0-9_]+)/)?.[1],
        version: idl.match(/\/\/ Version:\s*([^\n]+)/)?.[1]?.trim(),
        hash: idl.match(/\/\/ Hash:\s*([a-f0-9]+)/)?.[1],
        idl,
        files: [],
      };
    }),
  );
}

function normalizedSchemaIdentity(type) {
  const schemaHash = ArrayBuffer.isView(type?.schemaHash)
    ? Array.from(type.schemaHash)
    : type?.schemaHash;
  return {
    schemaName: type?.schemaName,
    fileIdentifier: type?.fileIdentifier,
    rootTypeName: type?.rootTypeName,
    schemaVersion: type?.schemaVersion,
    schemaHash,
  };
}

function assertCanonicalAlignedPair(port, location) {
  assert.equal(
    port.acceptedTypeSets?.length,
    1,
    `${location} must declare exactly one canonical SDS type set`,
  );
  const allowedTypes = port.acceptedTypeSets[0]?.allowedTypes ?? [];
  assert.equal(
    allowedTypes.length,
    2,
    `${location} must declare exactly a FlatBuffer/aligned-binary pair`,
  );
  const byWireFormat = new Map(
    allowedTypes.map((type) => [type.wireFormat, type]),
  );
  assert.deepEqual(
    [...byWireFormat.keys()].sort(),
    ["aligned-binary", "flatbuffer"],
    `${location} must support canonical FlatBuffer and aligned-binary`,
  );
  const canonical = byWireFormat.get("flatbuffer");
  const aligned = byWireFormat.get("aligned-binary");
  assert.deepEqual(
    normalizedSchemaIdentity(aligned),
    normalizedSchemaIdentity(canonical),
    `${location} representations must have identical SDS schema identity`,
  );
  assert.equal(
    Number.isSafeInteger(aligned.byteLength) && aligned.byteLength > 0,
    true,
    `${location} aligned representation must declare byteLength`,
  );
  assert.equal(
    Number.isSafeInteger(aligned.requiredAlignment) &&
      aligned.requiredAlignment > 0,
    true,
    `${location} aligned representation must declare requiredAlignment`,
  );
}

test("FlatSQL is an independently signed isomorphic SDN WASM node", async () => {
  const manifestBytes = await readRequired(
    manifestPath,
    "canonical FlatSQL plugin manifest",
  );
  const manifest = JSON.parse(manifestBytes.toString("utf8"));

  assert.equal(manifest.pluginId, "com.digitalarsenal.flatsql.store");
  assert.deepEqual(
    [...(manifest.runtimeTargets ?? [])].sort(),
    ["browser", "wasmedge"],
    "the same node artifact must target browser and WasmEdge",
  );
  assert.deepEqual(manifest.invokeSurfaces, ["direct"]);
  assert.deepEqual(manifest.capabilities ?? [], []);
  assert.deepEqual(manifest.externalInterfaces ?? [], []);

  const methods = new Map(
    (manifest.methods ?? []).map((method) => [method.methodId, method]),
  );
  for (const methodId of requiredMethods) {
    assert.ok(methods.has(methodId), `missing FlatSQL method ${methodId}`);
  }
  for (const [methodId, method] of methods) {
    for (const [portsKey, direction] of [
      ["inputPorts", "input"],
      ["outputPorts", "output"],
    ]) {
      for (const port of method[portsKey] ?? []) {
        assertCanonicalAlignedPair(
          port,
          `${methodId} ${direction} ${port.portId ?? "<unnamed>"}`,
        );
      }
    }
  }

  const { validateManifestWithStandards, validateArtifactWithStandards } =
    await importSdk("src/compliance/index.js");
  const catalog = await loadBoundaryCatalog();
  const manifestReport = await validateManifestWithStandards(manifest, {
    catalog,
    sourceName: manifestPath,
  });
  assert.equal(
    manifestReport.ok,
    true,
    JSON.stringify(manifestReport.errors, null, 2),
  );

  const artifactBytes = await readRequired(
    artifactPath,
    "isomorphic FlatSQL node artifact",
  );
  const artifactReport = await validateArtifactWithStandards({
    manifest,
    wasmBytes: artifactBytes,
    catalog,
    sourceName: artifactPath,
  });
  assert.equal(
    artifactReport.ok,
    true,
    JSON.stringify(artifactReport.errors, null, 2),
  );
  for (const exportName of requiredExports) {
    assert.ok(
      artifactReport.exportNames.includes(exportName),
      `FlatSQL node artifact is missing ${exportName}`,
    );
  }
  assert.deepEqual(
    [...artifactReport.exportNames].sort(),
    productionExportAllowlist,
    "the production node must not export internal FlatSQL or test APIs",
  );

  const { inspectModule } = await importSdk("src/host/isomorphicLoader.js");
  const inspection = await inspectModule(artifactBytes);
  for (const imported of inspection.imports) {
    const qualifiedName = `${imported.module}.${imported.name}`;
    assert.equal(
      forbiddenHostLinks.some((name) => qualifiedName.includes(name)),
      false,
      `FlatSQL must execute in this node, not through host import ${qualifiedName}`,
    );
  }

  const artifactText = new TextDecoder("latin1").decode(artifactBytes);
  for (const forbidden of forbiddenHostLinks) {
    assert.equal(
      artifactText.includes(forbidden),
      false,
      `FlatSQL artifact contains forbidden host-link marker ${forbidden}`,
    );
  }

  const publisher = JSON.parse(
    (
      await readRequired(publisherPath, "FlatSQL publisher trust record")
    ).toString("utf8"),
  );
  assert.equal(publisher.algorithm, "ed25519");
  assert.match(publisher.publicKeyHex, /^[0-9a-f]{64}$/);
  const expectedBuildMode =
    process.env.FLATSQL_NODE_EXPECT_BUILD_MODE ?? "development";
  assert.match(expectedBuildMode, /^(development|production)$/);
  assert.equal(
    publisher.developmentOnly,
    expectedBuildMode === "development",
    `publisher trust must match explicit ${expectedBuildMode} test mode`,
  );
  assert.equal(publisher.buildMode, expectedBuildMode);

  const { verifyModuleArtifact } = await importSdk("src/bundle/signing.js");
  const verified = await verifyModuleArtifact(artifactBytes, {
    trustedPublicKeys: [publisher.publicKeyHex],
    requireSignature: true,
  });
  assert.equal(verified.verified, true);
  assert.equal(verified.signed, true);
  assert.equal(
    verified.signatureScope,
    "bundle",
    "the signature must bind the PLG and every package member",
  );

  const artifactMetadata = JSON.parse(
    (
      await readRequired(
        artifactMetadataPath,
        "FlatSQL artifact hash metadata",
      )
    ).toString("utf8"),
  );
  const exactArtifactHash = createHash("sha256")
    .update(artifactBytes)
    .digest("hex");
  assert.equal(artifactMetadata.sha256, exactArtifactHash);
  assert.equal(
    artifactMetadata.canonicalModuleHash,
    verified.canonicalModuleHashHex,
  );

  const { locateEmbeddedPlgManifest } = await importSdk(
    "src/compliance/index.js",
  );
  const embedded = locateEmbeddedPlgManifest(artifactBytes, {
    expectedPluginId: manifest.pluginId,
    expectedVersion: manifest.version,
  });
  assert.ok(embedded, "signed FlatSQL artifact must contain a canonical PLG");
  assert.equal(embedded.decoded.pluginId, manifest.pluginId);
  assert.equal(embedded.decoded.version, manifest.version);
  const { encodePluginManifest } = await importSdk("src/manifest/index.js");
  assert.deepEqual(
    Buffer.from(embedded.bytes),
    Buffer.from(encodePluginManifest(manifest)),
    "the signed artifact must embed the exact canonical source manifest",
  );
});
