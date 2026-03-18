// SPDX-License-Identifier: Apache-2.0
// Copied from the FlatBuffers runtime (https://github.com/google/flatbuffers) to avoid a network dependency.
export { FILE_IDENTIFIER_LENGTH, SIZEOF_INT, SIZEOF_SHORT, SIZE_PREFIX_LENGTH, } from './constants.js';
export { float32, float64, int32, isLittleEndian } from './utils.js';
export { Builder } from './builder.js';
export { ByteBuffer } from './byte-buffer.js';
export { Encoding } from './encoding.js';
