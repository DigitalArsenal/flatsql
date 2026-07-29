describe('published package exports', () => {
  test('root and standalone subpaths expose production runtime entrypoints', async () => {
    const loadPackageExport = (specifier: string) => import(specifier);
    const root = await loadPackageExport('flatsql');
    const artifacts = await loadPackageExport('flatsql/artifacts');
    const standaloneArtifacts = await loadPackageExport('flatsql/artifacts/standalone');
    const response = await loadPackageExport('flatsql/response');
    const wasmedge = await loadPackageExport('flatsql/standalone/wasmedge');
    const standalone = await loadPackageExport('flatsql/standalone');

    // The root entry is browser-safe: node-only builders moved behind the
    // 'flatsql/artifacts' and 'flatsql/standalone/wasmedge' subpath exports
    // (they pull node:sqlite / node:os / node:child_process).
    expect(root.FlatSQLDatabase).toBeDefined();
    expect(root.createStandaloneArtifactBuilder).toBeUndefined();
    expect(typeof artifacts.FlatSQLArtifactBuilder).toBe('function');
    expect(typeof root.createQueryResponseArtifact).toBe('function');
    expect(typeof response.createQueryResponseArtifact).toBe('function');
    expect(typeof response.MemoryResponseArtifactCache).toBe('function');
    expect(typeof standaloneArtifacts.createStandaloneArtifactBuilder).toBe('function');
    expect(typeof wasmedge.buildFlatSQLWasmEdgeRunner).toBe('function');
    expect(typeof wasmedge.createFlatSQLWasmEdgeProcessRuntime).toBe('function');
    expect(typeof standalone.loadFlatSQLStandalone).toBe('function');
  });
});
