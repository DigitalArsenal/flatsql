import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import {
  mkdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const nodeDirectory = path.dirname(fileURLToPath(import.meta.url));
const flatsqlDirectory = path.resolve(nodeDirectory, "../..");
const mainPackagesDirectory = path.resolve(flatsqlDirectory, "..");
const repositoriesDirectory = path.resolve(mainPackagesDirectory, "..");
const sdkDirectory = process.env.SPACE_DATA_MODULE_SDK_ROOT
  ? path.resolve(process.env.SPACE_DATA_MODULE_SDK_ROOT)
  : path.join(
      repositoriesDirectory,
      "ancillary-packages/space-data-module-sdk",
    );
const standardsDirectory = path.join(
  mainPackagesDirectory,
  "spacedatastandards.org",
);
const flatbuffersDirectory = path.join(mainPackagesDirectory, "flatbuffers");
const flatcPath = path.join(flatbuffersDirectory, "build/flatc");
const cppDirectory = path.join(flatsqlDirectory, "cpp");
const buildDirectory = path.join(nodeDirectory, ".build");
const generatedDirectory = path.join(buildDirectory, "generated");
const cmakeBuildDirectory = path.join(buildDirectory, "cmake");
const distDirectory = path.join(nodeDirectory, "dist/isomorphic");
const manifestPath = path.join(nodeDirectory, "plugin-manifest.json");

const developmentSigningSeed = "41".repeat(32);
const buildMode = process.env.FLATSQL_NODE_BUILD_MODE;

function signingConfiguration() {
  if (buildMode !== "development" && buildMode !== "production") {
    throw new Error(
      "FLATSQL_NODE_BUILD_MODE must explicitly be development or production",
    );
  }
  if (buildMode === "development") {
    return {
      signingSeed: developmentSigningSeed,
      signingKeyId: "flatsql-node-development",
      developmentOnly: true,
    };
  }
  const signingSeed = process.env.FLATSQL_NODE_SIGNING_SEED_HEX;
  const signingKeyId = process.env.FLATSQL_NODE_SIGNING_KEY_ID;
  if (!signingSeed || !signingKeyId) {
    throw new Error(
      "production build requires FLATSQL_NODE_SIGNING_SEED_HEX and FLATSQL_NODE_SIGNING_KEY_ID",
    );
  }
  if (!/^[0-9a-fA-F]{64}$/.test(signingSeed)) {
    throw new Error("FLATSQL_NODE_SIGNING_SEED_HEX must be exactly 32 bytes");
  }
  if (
    signingSeed.toLowerCase() === developmentSigningSeed ||
    signingKeyId === "flatsql-node-development"
  ) {
    throw new Error("production build refuses the embedded development signer");
  }
  return {
    signingSeed: signingSeed.toLowerCase(),
    signingKeyId,
    developmentOnly: false,
  };
}

function sdkUrl(relativePath) {
  return pathToFileURL(path.join(sdkDirectory, relativePath)).href;
}

async function run(command, args, options = {}) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? flatsqlDirectory,
      env: { ...process.env, ...(options.env ?? {}) },
      stdio: "inherit",
    });
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(
        new Error(
          `${command} exited with ${code ?? `signal ${signal ?? "unknown"}`}`,
        ),
      );
    });
  });
}

function rewriteHeaderGuard(source, schemaCode) {
  return source.replaceAll(
    "FLATBUFFERS_GENERATED_MAIN_H_",
    `FLATBUFFERS_GENERATED_SDS_${schemaCode}_MAIN_H_`,
  );
}

function guardAlignedRuntime(source) {
  const startToken = "namespace flatbuffers {\nnamespace aligned_runtime {";
  const endToken = "}  // namespace aligned_runtime\n}  // namespace flatbuffers";
  const start = source.indexOf(startToken);
  const end = source.indexOf(endToken, start);
  if (start < 0 || end < 0) {
    throw new Error("generated aligned header is missing the shared runtime block");
  }
  const blockEnd = end + endToken.length;
  return `${source.slice(0, start)}#ifndef FLATSQL_SDS_ALIGNED_RUNTIME_DEFINED\n#define FLATSQL_SDS_ALIGNED_RUNTIME_DEFINED\n${source.slice(start, blockEnd)}\n#endif\n${source.slice(blockEnd)}`;
}

async function generateBoundaryHeaders(schemaCode) {
  const outputDirectory = path.join(generatedDirectory, `sds/${schemaCode}`);
  const schemaPath = path.join(
    standardsDirectory,
    `schema/${schemaCode}/main.fbs`,
  );
  await mkdir(outputDirectory, { recursive: true });
  await run(flatcPath, [
    "--no-warnings",
    "--cpp",
    "--gen-object-api",
    "-o",
    outputDirectory,
    schemaPath,
  ]);
  const generatedPath = path.join(outputDirectory, "main_generated.h");
  await writeFile(
    generatedPath,
    rewriteHeaderGuard(await readFile(generatedPath, "utf8"), schemaCode),
  );
  await run(flatcPath, [
    "--no-warnings",
    "--cpp",
    "--aligned",
    "-o",
    outputDirectory,
    schemaPath,
  ]);
  const alignedPath = path.join(outputDirectory, "main_aligned.h");
  await writeFile(
    alignedPath,
    guardAlignedRuntime(await readFile(alignedPath, "utf8")),
  );
}

async function main() {
  const { signingSeed, signingKeyId, developmentOnly } =
    signingConfiguration();
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  const [{ generateEmbeddedManifestSource }, invokeGlue, flatcSupport, bundle] =
    await Promise.all([
      import(sdkUrl("src/embeddedManifest.js")),
      import(sdkUrl("src/compiler/invokeGlue.js")),
      import(sdkUrl("src/compiler/flatcSupport.js")),
      import(sdkUrl("src/bundle/index.js")),
    ]);
  const { encodePluginManifest } = await import(
    sdkUrl("src/manifest/index.js")
  );

  await rm(buildDirectory, { recursive: true, force: true });
  await mkdir(generatedDirectory, { recursive: true });
  await mkdir(distDirectory, { recursive: true });

  const invokeHeaders = await flatcSupport.getInvokeCppSchemaHeaders();
  for (const [relativePath, source] of Object.entries(invokeHeaders)) {
    const destination = path.join(generatedDirectory, relativePath);
    await mkdir(path.dirname(destination), { recursive: true });
    await writeFile(destination, source);
  }
  await writeFile(
    path.join(generatedDirectory, "space_data_module_invoke.h"),
    invokeGlue.generateInvokeSupportHeader(),
  );
  await writeFile(
    path.join(generatedDirectory, "invoke_support.cpp"),
    invokeGlue.generateInvokeSupportSource({
      manifest,
      includeCommandMain: false,
    }),
  );
  await writeFile(
    path.join(generatedDirectory, "plugin_manifest_exports.c"),
    generateEmbeddedManifestSource({ manifest, format: "plg" }),
  );
  await writeFile(
    path.join(generatedDirectory, "private_capi_exports.h"),
    [
      "#include <emscripten.h>",
      "#undef EMSCRIPTEN_KEEPALIVE",
      "#define EMSCRIPTEN_KEEPALIVE",
      "",
    ].join("\n"),
  );
  await Promise.all([
    generateBoundaryHeaders("FSO"),
    generateBoundaryHeaders("FSB"),
  ]);

  await run("emcmake", [
    "cmake",
    "-S",
    cppDirectory,
    "-B",
    cmakeBuildDirectory,
    `-DFLATSQL_SDN_NODE_GENERATED_DIR=${generatedDirectory}`,
    "-DCMAKE_BUILD_TYPE=Release",
  ]);
  await run("cmake", [
    "--build",
    cmakeBuildDirectory,
    "--target",
    "flatsql_sdn_node",
    "--parallel",
  ]);

  const rawWasmPath = path.join(
    cmakeBuildDirectory,
    "flatsql-sdn-node.wasm",
  );
  const rawWasm = new Uint8Array(await readFile(rawWasmPath));
  const manifestBytes = encodePluginManifest(manifest);
  const withManifest = bundle.appendWasmCustomSection(
    rawWasm,
    bundle.SDS_MANIFEST_SECTION_NAME,
    manifestBytes,
  );
  const signed = await bundle.signModuleArtifact(withManifest, {
    privateKeySeedHex: signingSeed,
    keyId: signingKeyId,
    signatureScope: "bundle",
  });

  const artifactPath = path.join(distDirectory, "module.wasm");
  await writeFile(artifactPath, signed.wasmBytes);
  const exactSha256 = createHash("sha256")
    .update(signed.wasmBytes)
    .digest("hex");
  await writeFile(
    path.join(distDirectory, "artifact.json"),
    `${JSON.stringify(
      {
        sha256: exactSha256,
        canonicalModuleHash: signed.canonicalModuleHashHex,
        signedHash: signed.signedHashHex,
        signatureScope: "bundle",
        keyId: signingKeyId,
        buildMode,
      },
      null,
      2,
    )}\n`,
  );
  await writeFile(
    path.join(nodeDirectory, "publisher.json"),
    `${JSON.stringify(
      {
        algorithm: "ed25519",
        keyId: signingKeyId,
        publicKeyHex: signed.signature.publicKeyHex,
        developmentOnly,
        buildMode,
      },
      null,
      2,
    )}\n`,
  );

  process.stdout.write(
    `Built signed FlatSQL node ${exactSha256} (${signed.wasmBytes.byteLength} bytes)\n`,
  );
}

main().catch((error) => {
  process.stderr.write(`${error.stack ?? error.message}\n`);
  process.exitCode = 1;
});
