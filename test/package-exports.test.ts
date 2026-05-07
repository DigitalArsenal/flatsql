describe('published package exports', () => {
  test('root and standalone subpaths expose production runtime entrypoints', async () => {
    const loadPackageExport = (specifier: string) => import(specifier);
    const root = await loadPackageExport('flatsql');
    const standaloneArtifacts = await loadPackageExport('flatsql/artifacts/standalone');
    const wasmedge = await loadPackageExport('flatsql/standalone/wasmedge');
    const standalone = await loadPackageExport('flatsql/standalone');

    expect(typeof root.createStandaloneArtifactBuilder).toBe('function');
    expect(typeof standaloneArtifacts.createStandaloneArtifactBuilder).toBe('function');
    expect(typeof wasmedge.buildFlatSQLWasmEdgeRunner).toBe('function');
    expect(typeof wasmedge.createFlatSQLWasmEdgeProcessRuntime).toBe('function');
    expect(typeof standalone.loadFlatSQLStandalone).toBe('function');
  });
});
