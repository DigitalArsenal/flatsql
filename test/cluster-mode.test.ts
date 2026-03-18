import {
  detectClusterEnvironment,
  describeClusterSupport,
  isClusterModeSupported,
} from '../src/index.js';

describe('Cluster mode detection', () => {
  test('Node runtime fails closed when browser features are absent', () => {
    const env = detectClusterEnvironment();
    expect(env.hasCrossOriginIsolation).toBe(false);
    expect(env.hasOpfs).toBe(false);
    expect(isClusterModeSupported(env)).toBe(false);

    const summary = describeClusterSupport(env);
    expect(summary).toMatch(/missing/i);
    expect(summary).toMatch(/cross-origin isolation/);
  });
});
