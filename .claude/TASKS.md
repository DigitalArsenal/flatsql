# FlatSQL Build & Publish Tasks

This document outlines all tasks required to build, test, and publish FlatSQL.

## Prerequisites

Before any build tasks, ensure these are installed:

```bash
# Node.js 18+
node --version

# Emscripten (for WASM builds)
emcc --version

# CMake 3.20+
cmake --version

# DA-FlatBuffers (clone if not present)
ls ../flatbuffers || git clone https://github.com/DigitalArsenal/flatbuffers.git ../flatbuffers
```

---

## Task 1: Build TypeScript

Compiles TypeScript source to JavaScript.

```bash
npm install
npm run build
```

**Output:** `dist/` directory with compiled JS and type definitions

**Verify:**
```bash
ls dist/index.js dist/index.d.ts
```

---

## Task 2: Build WASM

Compiles C++ to WebAssembly using Emscripten.

```bash
cd cpp

# Configure
emcmake cmake -B build-wasm -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build build-wasm --config Release

# Copy to wasm/ and docs/
cp build-wasm/flatsql.js build-wasm/flatsql.wasm ../wasm/
cp build-wasm/flatsql.js build-wasm/flatsql.wasm ../docs/
```

**Output:**
- `wasm/flatsql.js` - WASM loader
- `wasm/flatsql.wasm` - WebAssembly binary
- `docs/flatsql.js` and `docs/flatsql.wasm` - For GitHub Pages demo

**Verify:**
```bash
ls -la wasm/flatsql.js wasm/flatsql.wasm
node wasm/test-node.mjs
```

---

## Task 3: Build Native C++ (for testing)

Compiles native C++ for local testing.

```bash
cd cpp

# Configure
cmake -B build -DCMAKE_BUILD_TYPE=Debug

# Build
cmake --build build --config Debug -j8

# Run tests
cd build && ctest --output-on-failure
```

**Output:** Native test executables in `cpp/build/`

---

## Task 4: Run All Tests

```bash
# TypeScript tests
npm test

# WASM tests
node wasm/test-node.mjs
node wasm/test-queries.mjs
node wasm/test-integration.mjs

# Native C++ tests (if built)
cd cpp/build && ctest --output-on-failure
```

---

## Task 5: Prepare for npm Publish

Before publishing, verify the package contents:

```bash
# See what will be published
npm pack --dry-run

# Check package size
npm pack && ls -la flatsql-*.tgz && rm flatsql-*.tgz
```

**Expected files in package:**
- `dist/` - Compiled TypeScript
- `wasm/*.js` - WASM loader and API
- `wasm/*.wasm` - WebAssembly binary
- `README.md`
- `LICENSE`
- `package.json`

---

## Task 6: Publish to npm

```bash
# Login to npm (first time only)
npm login

# Publish (dry run first)
npm publish --dry-run

# Publish for real
npm publish --access public
```

**Post-publish verification:**
```bash
npm info flatsql
```

---

## Task 7: Version Bump

For releasing new versions:

```bash
# Patch (0.1.0 -> 0.1.1)
npm version patch

# Minor (0.1.0 -> 0.2.0)
npm version minor

# Major (0.1.0 -> 1.0.0)
npm version major

# Push tags
git push && git push --tags
```

---

## Task 8: Update Demo Site

After WASM rebuild, update GitHub Pages:

```bash
# Copy WASM files to docs/
cp wasm/flatsql.js wasm/flatsql.wasm docs/

# Commit and push
git add docs/flatsql.js docs/flatsql.wasm
git commit -m "Update WASM for demo"
git push
```

Demo will be live at: https://digitalarsenal.github.io/flatsql/

---

## Full Release Checklist

1. [ ] Update version in `package.json`
2. [ ] Build TypeScript: `npm run build`
3. [ ] Build WASM: See Task 2
4. [ ] Run all tests: See Task 4
5. [ ] Update CHANGELOG.md (if exists)
6. [ ] Commit all changes
7. [ ] Create git tag: `git tag v0.x.x`
8. [ ] Push: `git push && git push --tags`
9. [ ] Publish to npm: `npm publish --access public`
10. [ ] Verify npm package: `npm info flatsql`
11. [ ] Verify demo site works

---

## Quick Commands Reference

| Task | Command |
|------|---------|
| Install deps | `npm install` |
| Build TS | `npm run build` |
| Build WASM | `cd cpp && emcmake cmake -B build-wasm && cmake --build build-wasm` |
| Test TS | `npm test` |
| Test WASM | `node wasm/test-node.mjs` |
| Dev server | `npm run serve` |
| Publish | `npm publish --access public` |

---

## Troubleshooting

### WASM build fails with "FlatBuffers not found"
```bash
# Clone DA-FlatBuffers to sibling directory
git clone https://github.com/DigitalArsenal/flatbuffers.git ../flatbuffers
```

### Tests fail with SEGFAULT on Linux
This is a known issue with SQLite virtual table callbacks on CI runners. Tests pass locally on macOS. WASM build is not affected.

### npm publish fails with 403
```bash
# Make sure you're logged in
npm login

# Check package name isn't taken
npm info flatsql
```
