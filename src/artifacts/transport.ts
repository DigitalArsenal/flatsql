export function sizePrefixedByteLength(buffers: Uint8Array[]): number {
  let totalLength = 0;
  for (const buffer of buffers) {
    totalLength += 4 + buffer.length;
  }
  return totalLength;
}

export function writeSizePrefixedStream(target: Uint8Array, buffers: Uint8Array[]): number {
  let offset = 0;
  const view = new DataView(target.buffer, target.byteOffset, target.byteLength);
  for (const buffer of buffers) {
    view.setUint32(offset, buffer.length, true);
    offset += 4;
    target.set(buffer, offset);
    offset += buffer.length;
  }
  return offset;
}

export function forEachSizePrefixedBuffer(
  stream: Uint8Array,
  visitor: (buffer: Uint8Array, index: number) => void
): number {
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  let offset = 0;
  let index = 0;

  while (offset < stream.byteLength) {
    if (stream.byteLength - offset < 4) {
      throw new Error(`Invalid size-prefixed stream: truncated frame header at offset ${offset}`);
    }

    const size = view.getUint32(offset, true);
    offset += 4;
    if (size > stream.byteLength - offset) {
      throw new Error(`Invalid size-prefixed stream: truncated frame at index ${index}`);
    }

    visitor(stream.subarray(offset, offset + size), index);
    offset += size;
    index += 1;
  }

  return index;
}

export function decodeSizePrefixedStream(stream: Uint8Array): Uint8Array[] {
  const buffers: Uint8Array[] = [];
  forEachSizePrefixedBuffer(stream, (buffer) => {
    buffers.push(buffer);
  });

  return buffers;
}
