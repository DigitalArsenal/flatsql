// B-Tree types for FlatBuffers-SQLite
export var KeyType;
(function (KeyType) {
    KeyType[KeyType["Null"] = 0] = "Null";
    KeyType[KeyType["Int"] = 1] = "Int";
    KeyType[KeyType["Float"] = 2] = "Float";
    KeyType[KeyType["String"] = 3] = "String";
    KeyType[KeyType["Bytes"] = 4] = "Bytes";
})(KeyType || (KeyType = {}));
// Compare two keys of the same type
export function compareKeys(a, b, keyType) {
    if (a === null && b === null)
        return 0;
    if (a === null)
        return -1;
    if (b === null)
        return 1;
    switch (keyType) {
        case KeyType.Int:
        case KeyType.Float:
            const numA = a;
            const numB = b;
            if (numA < numB)
                return -1;
            if (numA > numB)
                return 1;
            return 0;
        case KeyType.String:
            const strA = a;
            const strB = b;
            if (strA < strB)
                return -1;
            if (strA > strB)
                return 1;
            return 0;
        case KeyType.Bytes:
            const bytesA = a;
            const bytesB = b;
            const minLen = Math.min(bytesA.length, bytesB.length);
            for (let i = 0; i < minLen; i++) {
                if (bytesA[i] < bytesB[i])
                    return -1;
                if (bytesA[i] > bytesB[i])
                    return 1;
            }
            if (bytesA.length < bytesB.length)
                return -1;
            if (bytesA.length > bytesB.length)
                return 1;
            return 0;
        default:
            return 0;
    }
}
//# sourceMappingURL=types.js.map