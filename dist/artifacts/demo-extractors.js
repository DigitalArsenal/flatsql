function readUint32(data, offset) {
    return new DataView(data.buffer, data.byteOffset, data.byteLength).getUint32(offset, true);
}
function readInt32(data, offset) {
    return new DataView(data.buffer, data.byteOffset, data.byteLength).getInt32(offset, true);
}
function getFieldOffset(data, fieldIndex) {
    const rootOffset = readUint32(data, 0);
    const root = rootOffset;
    const vtableOffset = readInt32(data, root);
    const vtable = root - vtableOffset;
    const vtableSize = new DataView(data.buffer, data.byteOffset, data.byteLength).getUint16(vtable, true);
    const entryOffset = vtable + 4 + fieldIndex * 2;
    if (entryOffset + 2 > vtable + vtableSize) {
        return 0;
    }
    return new DataView(data.buffer, data.byteOffset, data.byteLength).getUint16(entryOffset, true);
}
function readStringField(data, fieldIndex) {
    const rootOffset = readUint32(data, 0);
    const root = rootOffset;
    const fieldOffset = getFieldOffset(data, fieldIndex);
    if (fieldOffset === 0) {
        return '';
    }
    const relative = readUint32(data, root + fieldOffset);
    const stringStart = root + fieldOffset + relative;
    const stringLength = readUint32(data, stringStart);
    const stringBytes = data.subarray(stringStart + 4, stringStart + 4 + stringLength);
    return new TextDecoder().decode(stringBytes);
}
export const demoExtractors = {
    User(data, fieldName) {
        const rootOffset = readUint32(data, 0);
        const root = rootOffset;
        switch (fieldName) {
            case 'id': {
                const fieldOffset = getFieldOffset(data, 0);
                return fieldOffset === 0 ? 0 : readInt32(data, root + fieldOffset);
            }
            case 'name':
                return readStringField(data, 1);
            case 'email':
                return readStringField(data, 2);
            case 'age': {
                const fieldOffset = getFieldOffset(data, 3);
                return fieldOffset === 0 ? 0 : readInt32(data, root + fieldOffset);
            }
            default:
                return null;
        }
    },
    Post(data, fieldName) {
        const rootOffset = readUint32(data, 0);
        const root = rootOffset;
        switch (fieldName) {
            case 'id': {
                const fieldOffset = getFieldOffset(data, 0);
                return fieldOffset === 0 ? 0 : readInt32(data, root + fieldOffset);
            }
            case 'user_id': {
                const fieldOffset = getFieldOffset(data, 1);
                return fieldOffset === 0 ? 0 : readInt32(data, root + fieldOffset);
            }
            case 'title':
                return readStringField(data, 2);
            default:
                return null;
        }
    },
};
//# sourceMappingURL=demo-extractors.js.map