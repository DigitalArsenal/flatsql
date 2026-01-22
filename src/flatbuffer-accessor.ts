// FlatBuffer accessor implementation using flatc-wasm
// Provides field extraction and buffer building capabilities

import { FlatBufferAccessor } from './core/database.js';

// Type for the FlatcRunner from flatc-wasm
interface FlatcRunner {
  generateBinary(schema: SchemaInput, json: string): Uint8Array;
  generateJSON(schema: SchemaInput, binary: { path: string; data: Uint8Array }): string;
  generateCode(schema: SchemaInput, language: string, options?: any): Record<string, string>;
}

interface SchemaInput {
  entry: string;
  files: Record<string, string>;
}

// Dynamic accessor that uses flatc-wasm for JSON<->Binary conversion
// This is the bridge between SQL queries and FlatBuffer data
export class FlatcAccessor implements FlatBufferAccessor {
  private flatc: FlatcRunner;
  private schemaInput: SchemaInput;
  private tableSchemas: Map<string, SchemaInput> = new Map();

  constructor(flatc: FlatcRunner, schemaSource: string, schemaName: string = 'schema.fbs') {
    this.flatc = flatc;
    this.schemaInput = {
      entry: `/${schemaName}`,
      files: {
        [`/${schemaName}`]: schemaSource,
      },
    };
  }

  // Register a table-specific schema (for multi-table scenarios)
  registerTableSchema(tableName: string, schemaSource: string): void {
    this.tableSchemas.set(tableName, {
      entry: `/${tableName}.fbs`,
      files: {
        [`/${tableName}.fbs`]: schemaSource,
      },
    });
  }

  // Get the schema for a table
  private getSchema(tableName?: string): SchemaInput {
    if (tableName && this.tableSchemas.has(tableName)) {
      return this.tableSchemas.get(tableName)!;
    }
    return this.schemaInput;
  }

  // Extract a field value from FlatBuffer binary data
  getField(data: Uint8Array, path: string[]): any {
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
    } catch (error) {
      // Field not present or extraction failed
      return null;
    }
  }

  // Build a FlatBuffer from field values (JSON object)
  buildBuffer(tableName: string, fields: Record<string, any>): Uint8Array {
    const schema = this.getSchema(tableName);
    const json = JSON.stringify(fields);
    return this.flatc.generateBinary(schema, json);
  }

  // Convert FlatBuffer to JSON object
  toJSON(data: Uint8Array, tableName?: string): Record<string, any> {
    const schema = this.getSchema(tableName);
    const json = this.flatc.generateJSON(schema, {
      path: '/data.bin',
      data,
    });
    return JSON.parse(json);
  }

  // Convert JSON object to FlatBuffer
  fromJSON(obj: Record<string, any>, tableName?: string): Uint8Array {
    const schema = this.getSchema(tableName);
    return this.flatc.generateBinary(schema, JSON.stringify(obj));
  }
}

// Simple accessor for when you have generated TypeScript code
// Uses direct field access (much faster than JSON round-trip)
export class DirectAccessor implements FlatBufferAccessor {
  private accessors: Map<string, (data: Uint8Array, path: string[]) => any> = new Map();
  private builders: Map<string, (fields: Record<string, any>) => Uint8Array> = new Map();

  // Register a field accessor for a table
  registerAccessor(
    tableName: string,
    accessor: (data: Uint8Array, path: string[]) => any
  ): void {
    this.accessors.set(tableName, accessor);
  }

  // Register a buffer builder for a table
  registerBuilder(
    tableName: string,
    builder: (fields: Record<string, any>) => Uint8Array
  ): void {
    this.builders.set(tableName, builder);
  }

  getField(data: Uint8Array, path: string[]): any {
    // Try each registered accessor until one works
    for (const accessor of this.accessors.values()) {
      try {
        const result = accessor(data, path);
        if (result !== undefined) {
          return result;
        }
      } catch {
        // Try next accessor
      }
    }
    return null;
  }

  buildBuffer(tableName: string, fields: Record<string, any>): Uint8Array {
    const builder = this.builders.get(tableName);
    if (!builder) {
      throw new Error(`No builder registered for table: ${tableName}`);
    }
    return builder(fields);
  }
}
