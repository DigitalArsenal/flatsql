// FlatBuffer accessor implementation using flatc-wasm
// Provides field extraction and buffer building capabilities
// Dynamic accessor that uses flatc-wasm for JSON<->Binary conversion
// This is the bridge between SQL queries and FlatBuffer data
export class FlatcAccessor {
    flatc;
    schemaInput;
    tableSchemas = new Map();
    constructor(flatc, schemaSource, schemaName = 'schema.fbs') {
        this.flatc = flatc;
        this.schemaInput = {
            entry: `/${schemaName}`,
            files: {
                [`/${schemaName}`]: schemaSource,
            },
        };
    }
    // Register a table-specific schema (for multi-table scenarios)
    registerTableSchema(tableName, schemaSource) {
        this.tableSchemas.set(tableName, {
            entry: `/${tableName}.fbs`,
            files: {
                [`/${tableName}.fbs`]: schemaSource,
            },
        });
    }
    // Get the schema for a table
    getSchema(tableName) {
        if (tableName && this.tableSchemas.has(tableName)) {
            return this.tableSchemas.get(tableName);
        }
        return this.schemaInput;
    }
    // Extract a field value from FlatBuffer binary data
    getField(data, path) {
        try {
            // Convert FlatBuffer to JSON
            const json = this.flatc.generateJSON(this.schemaInput, {
                path: '/data.bin',
                data,
            });
            // Parse JSON and navigate path
            let obj = JSON.parse(json);
            for (const key of path) {
                if (obj === null || obj === undefined) {
                    return null;
                }
                obj = obj[key];
            }
            return obj;
        }
        catch (error) {
            // Field not present or extraction failed
            return null;
        }
    }
    // Build a FlatBuffer from field values (JSON object)
    buildBuffer(tableName, fields) {
        const schema = this.getSchema(tableName);
        const json = JSON.stringify(fields);
        return this.flatc.generateBinary(schema, json);
    }
    // Convert FlatBuffer to JSON object
    toJSON(data, tableName) {
        const schema = this.getSchema(tableName);
        const json = this.flatc.generateJSON(schema, {
            path: '/data.bin',
            data,
        });
        return JSON.parse(json);
    }
    // Convert JSON object to FlatBuffer
    fromJSON(obj, tableName) {
        const schema = this.getSchema(tableName);
        return this.flatc.generateBinary(schema, JSON.stringify(obj));
    }
}
// Simple accessor for when you have generated TypeScript code
// Uses direct field access (much faster than JSON round-trip)
export class DirectAccessor {
    accessors = new Map();
    builders = new Map();
    // Register a field accessor for a table
    registerAccessor(tableName, accessor) {
        this.accessors.set(tableName, accessor);
    }
    // Register a buffer builder for a table
    registerBuilder(tableName, builder) {
        this.builders.set(tableName, builder);
    }
    getField(data, path) {
        // Try each registered accessor until one works
        for (const accessor of this.accessors.values()) {
            try {
                const result = accessor(data, path);
                if (result !== undefined) {
                    return result;
                }
            }
            catch {
                // Try next accessor
            }
        }
        return null;
    }
    buildBuffer(tableName, fields) {
        const builder = this.builders.get(tableName);
        if (!builder) {
            throw new Error(`No builder registered for table: ${tableName}`);
        }
        return builder(fields);
    }
}
//# sourceMappingURL=flatbuffer-accessor.js.map