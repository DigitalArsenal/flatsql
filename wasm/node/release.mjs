import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const nodeDirectory = path.dirname(fileURLToPath(import.meta.url));

async function readJson(relativePath) {
  return JSON.parse(
    await readFile(path.join(nodeDirectory, relativePath), "utf8"),
  );
}

async function main() {
  if (process.argv.length !== 3 || process.argv[2] !== "--check") {
    throw new Error("usage: node release.mjs --check");
  }
  const [publisher, artifact, moduleBytes] = await Promise.all([
    readJson("publisher.json"),
    readJson("dist/isomorphic/artifact.json"),
    readFile(path.join(nodeDirectory, "dist/isomorphic/module.wasm")),
  ]);
  if (publisher.developmentOnly !== false) {
    throw new Error("development-only FlatSQL publisher cannot be released");
  }
  if (
    publisher.buildMode !== "production" ||
    artifact.buildMode !== "production"
  ) {
    throw new Error("FlatSQL release artifacts must be built in production mode");
  }
  if (
    !publisher.keyId ||
    publisher.keyId === "flatsql-node-development" ||
    publisher.keyId !== artifact.keyId
  ) {
    throw new Error("FlatSQL release publisher key identity is invalid");
  }
  const exactHash = createHash("sha256").update(moduleBytes).digest("hex");
  if (artifact.sha256 !== exactHash) {
    throw new Error("FlatSQL release artifact SHA-256 does not match module.wasm");
  }
  process.stdout.write(`FlatSQL production release verified ${exactHash}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error.stack ?? error.message}\n`);
  process.exitCode = 1;
});
