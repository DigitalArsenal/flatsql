export function sizePrefixedByteLength(buffers) {
    let totalLength = 0;
    for (const buffer of buffers) {
        totalLength += 4 + buffer.length;
    }
    return totalLength;
}
export function writeSizePrefixedStream(target, buffers) {
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
export function forEachSizePrefixedBuffer(stream, visitor) {
    const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
    let offset = 0;
    let index = 0;
    while (offset < stream.byteLength) {
        const size = view.getUint32(offset, true);
        offset += 4;
        visitor(stream.subarray(offset, offset + size), index);
        offset += size;
        index += 1;
    }
    return index;
}
export function decodeSizePrefixedStream(stream) {
    const buffers = [];
    forEachSizePrefixedBuffer(stream, (buffer) => {
        buffers.push(buffer);
    });
    return buffers;
}
//# sourceMappingURL=transport.js.map