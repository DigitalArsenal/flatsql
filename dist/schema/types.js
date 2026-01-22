// Schema types for FlatBuffers-SQLite
export var SQLColumnType;
(function (SQLColumnType) {
    SQLColumnType["NULL"] = "NULL";
    SQLColumnType["INTEGER"] = "INTEGER";
    SQLColumnType["REAL"] = "REAL";
    SQLColumnType["TEXT"] = "TEXT";
    SQLColumnType["BLOB"] = "BLOB";
})(SQLColumnType || (SQLColumnType = {}));
// FlatBuffer type to SQL type mapping
export function fbTypeToSQL(fbType) {
    const type = fbType.toLowerCase();
    // Integer types
    if (['bool', 'byte', 'ubyte', 'int8', 'uint8', 'short', 'ushort', 'int16', 'uint16',
        'int', 'uint', 'int32', 'uint32', 'long', 'ulong', 'int64', 'uint64'].includes(type)) {
        return SQLColumnType.INTEGER;
    }
    // Float types
    if (['float', 'float32', 'double', 'float64'].includes(type)) {
        return SQLColumnType.REAL;
    }
    // String type
    if (type === 'string') {
        return SQLColumnType.TEXT;
    }
    // Default to BLOB for complex types
    return SQLColumnType.BLOB;
}
//# sourceMappingURL=types.js.map