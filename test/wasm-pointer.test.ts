import { __testWasmPointerToByteOffset } from '../wasm/index.js';

describe('WASM pointer handling', () => {
  test('normalizes signed 32-bit pointers before typed-array access', () => {
    expect(__testWasmPointerToByteOffset(-1995635752)).toBe(2299331544);
    expect(__testWasmPointerToByteOffset(1024)).toBe(1024);
    expect(__testWasmPointerToByteOffset(0)).toBe(0);
  });
});
