import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const nodeDirectory = path.resolve(testDirectory, "..");

async function readJson(relativePath) {
  return JSON.parse(
    await readFile(path.join(nodeDirectory, relativePath), "utf8"),
  );
}

test("FlatSQL node package declares build, test, and guarded release assets", async () => {
  const packageJson = await readJson("package.json");
  assert.equal(packageJson.name, "@digitalarsenal/flatsql-wasm-node");
  assert.equal(packageJson.version, "2.0.0");
  assert.match(packageJson.scripts?.build ?? "", /FLATSQL_NODE_BUILD_MODE=development/);
  assert.match(packageJson.scripts?.test ?? "", /node --test/);
  assert.match(packageJson.scripts?.release ?? "", /FLATSQL_NODE_BUILD_MODE=production/);
  assert.match(packageJson.scripts?.release ?? "", /release\.mjs/);

  const ignore = await readFile(path.join(nodeDirectory, ".gitignore"), "utf8");
  assert.match(ignore, /^\.build\/$/m, "generated compiler state must stay untracked");

  const { stdout } = await execFileAsync(
    "npm",
    ["pack", "--dry-run", "--json", "--ignore-scripts"],
    { cwd: nodeDirectory },
  );
  const [{ files }] = JSON.parse(stdout);
  const packed = new Set(files.map((entry) => entry.path));
  for (const required of [
    "README.md",
    "build.mjs",
    "release.mjs",
    "package.json",
    "plugin-manifest.json",
    "publisher.json",
    "dist/isomorphic/artifact.json",
    "dist/isomorphic/module.wasm",
    "test/package-contract.test.mjs",
    "test/release-contract.test.mjs",
    "test/runtime-contract.test.mjs",
    "test/sds-boundary-contract.test.mjs",
  ]) {
    assert.equal(packed.has(required), true, `npm package is missing ${required}`);
  }
  assert.equal(
    [...packed].some((entry) => entry.startsWith(".build/")),
    false,
    "npm package must exclude generated compiler state",
  );
});

test("production build refuses the embedded development signer", async () => {
  const env = { ...process.env, FLATSQL_NODE_BUILD_MODE: "production" };
  delete env.FLATSQL_NODE_SIGNING_SEED_HEX;
  delete env.FLATSQL_NODE_SIGNING_KEY_ID;
  await assert.rejects(
    execFileAsync(process.execPath, [path.join(nodeDirectory, "build.mjs")], {
      cwd: nodeDirectory,
      env,
    }),
    (error) => {
      assert.match(
        `${error?.stdout ?? ""}${error?.stderr ?? ""}`,
        /production build requires FLATSQL_NODE_SIGNING_SEED_HEX and FLATSQL_NODE_SIGNING_KEY_ID/,
      );
      return true;
    },
  );
});

test("release guard rejects a developmentOnly publisher", async () => {
  await assert.rejects(
    execFileAsync(process.execPath, [path.join(nodeDirectory, "release.mjs"), "--check"], {
      cwd: nodeDirectory,
    }),
    (error) => {
      assert.match(
        `${error?.stdout ?? ""}${error?.stderr ?? ""}`,
        /development-only FlatSQL publisher cannot be released/,
      );
      return true;
    },
  );
});
