// Schema parser for FlatBuffers IDL and JSON Schema
// Converts to internal table definitions

import { ColumnDef, TableDef, DatabaseSchema, SQLColumnType, fbTypeToSQL } from './types.js';

// Security: Maximum schema source size to prevent DoS
const MAX_SCHEMA_SIZE = 1024 * 1024; // 1MB

// Security: Forbidden keys that could enable prototype pollution
const FORBIDDEN_KEYS = ['__proto__', 'constructor', 'prototype'];

/**
 * Safe JSON parse that prevents prototype pollution attacks.
 * Throws if the input contains dangerous keys or is too large.
 */
function safeJSONParse(source: string): unknown {
  if (source.length > MAX_SCHEMA_SIZE) {
    throw new Error(`Schema source exceeds maximum size of ${MAX_SCHEMA_SIZE} bytes`);
  }

  // Check for prototype pollution attempts in the raw string
  for (const key of FORBIDDEN_KEYS) {
    if (source.includes(`"${key}"`)) {
      throw new Error(`Schema contains forbidden key: ${key}`);
    }
  }

  const parsed = JSON.parse(source);

  // Recursively validate the parsed object
  validateObject(parsed, 0);

  return parsed;
}

/**
 * Recursively validate an object for dangerous patterns.
 * Limits nesting depth to prevent stack overflow.
 */
function validateObject(obj: unknown, depth: number): void {
  const MAX_DEPTH = 50;

  if (depth > MAX_DEPTH) {
    throw new Error(`Schema nesting depth exceeds maximum of ${MAX_DEPTH}`);
  }

  if (obj === null || typeof obj !== 'object') {
    return;
  }

  if (Array.isArray(obj)) {
    for (const item of obj) {
      validateObject(item, depth + 1);
    }
    return;
  }

  for (const key of Object.keys(obj)) {
    if (FORBIDDEN_KEYS.includes(key)) {
      throw new Error(`Schema contains forbidden key: ${key}`);
    }
    validateObject((obj as Record<string, unknown>)[key], depth + 1);
  }
}

/**
 * Type guard to check if a value is a valid JSON Schema object.
 */
function isValidJSONSchema(value: unknown): value is {
  type?: string;
  title?: string;
  properties?: Record<string, unknown>;
  definitions?: Record<string, unknown>;
  $defs?: Record<string, unknown>;
} {
  if (value === null || typeof value !== 'object') {
    return false;
  }
  const obj = value as Record<string, unknown>;

  // If type is present, it must be a string
  if (obj.type !== undefined && typeof obj.type !== 'string') {
    return false;
  }

  // If title is present, it must be a string
  if (obj.title !== undefined && typeof obj.title !== 'string') {
    return false;
  }

  // If properties is present, it must be an object
  if (obj.properties !== undefined && (typeof obj.properties !== 'object' || obj.properties === null)) {
    return false;
  }

  return true;
}

/**
 * Type guard to check if a value is a valid property definition.
 */
function isValidPropertyDef(value: unknown): value is {
  type?: string;
  enum?: unknown[];
  items?: { type?: string };
  default?: unknown;
  required?: boolean;
} {
  if (value === null || typeof value !== 'object') {
    return false;
  }
  const obj = value as Record<string, unknown>;

  if (obj.type !== undefined && typeof obj.type !== 'string') {
    return false;
  }

  return true;
}

interface FBField {
  name: string;
  type: string;
  isVector: boolean;
  defaultValue?: any;
  attributes?: string[];
}

interface FBTable {
  name: string;
  namespace: string;
  fields: FBField[];
  isStruct: boolean;
}

interface ParsedSchema {
  tables: FBTable[];
  enums: Map<string, string[]>;
  rootType?: string;
}

// Parse FlatBuffer IDL schema
export function parseFlatBufferIDL(source: string): ParsedSchema {
  const result: ParsedSchema = {
    tables: [],
    enums: new Map(),
  };

  let currentNamespace = '';
  const lines = source.split('\n');
  let i = 0;

  while (i < lines.length) {
    const line = lines[i].trim();

    // Skip empty lines and comments
    if (!line || line.startsWith('//')) {
      i++;
      continue;
    }

    // Namespace
    const nsMatch = line.match(/^namespace\s+([\w.]+)\s*;/);
    if (nsMatch) {
      currentNamespace = nsMatch[1];
      i++;
      continue;
    }

    // Root type
    const rootMatch = line.match(/^root_type\s+(\w+)\s*;/);
    if (rootMatch) {
      result.rootType = rootMatch[1];
      i++;
      continue;
    }

    // Enum
    const enumMatch = line.match(/^enum\s+(\w+)\s*:\s*\w+\s*\{/);
    if (enumMatch) {
      const enumName = enumMatch[1];
      const values: string[] = [];
      i++;

      while (i < lines.length) {
        const enumLine = lines[i].trim();
        if (enumLine === '}') {
          i++;
          break;
        }
        const valueMatch = enumLine.match(/^(\w+)/);
        if (valueMatch) {
          values.push(valueMatch[1]);
        }
        i++;
      }

      result.enums.set(enumName, values);
      continue;
    }

    // Table or Struct
    const tableMatch = line.match(/^(table|struct)\s+(\w+)\s*\{/);
    if (tableMatch) {
      const isStruct = tableMatch[1] === 'struct';
      const tableName = tableMatch[2];
      const fields: FBField[] = [];
      i++;

      while (i < lines.length) {
        const fieldLine = lines[i].trim();
        if (fieldLine === '}') {
          i++;
          break;
        }

        // Parse field: name: type (attributes) = default;
        const fieldMatch = fieldLine.match(/^(\w+)\s*:\s*(\[?\w+\]?)\s*(?:\((.*?)\))?\s*(?:=\s*(.+?))?\s*;/);
        if (fieldMatch) {
          const isVector = fieldMatch[2].startsWith('[');
          const typeName = isVector ? fieldMatch[2].slice(1, -1) : fieldMatch[2];
          const attrs = fieldMatch[3] ? fieldMatch[3].split(',').map(a => a.trim()) : [];
          const defaultVal = fieldMatch[4];

          fields.push({
            name: fieldMatch[1],
            type: typeName,
            isVector,
            defaultValue: defaultVal,
            attributes: attrs,
          });
        }
        i++;
      }

      result.tables.push({
        name: tableName,
        namespace: currentNamespace,
        fields,
        isStruct,
      });
      continue;
    }

    i++;
  }

  return result;
}

// Parse JSON Schema with security validation
export function parseJSONSchema(source: string): ParsedSchema {
  // Security: Use safe JSON parse to prevent prototype pollution
  const rawSchema = safeJSONParse(source);

  // Security: Validate schema structure
  if (!isValidJSONSchema(rawSchema)) {
    throw new Error('Invalid JSON Schema: expected an object with valid structure');
  }

  const schema = rawSchema;
  const result: ParsedSchema = {
    tables: [],
    enums: new Map(),
  };

  // Handle top-level object as a table
  if (schema.type === 'object' && schema.properties) {
    const tableName = schema.title || 'Root';
    const fields: FBField[] = [];

    // Security: Use Object.keys instead of Object.entries to avoid prototype chain
    const propNames = Object.keys(schema.properties);
    for (const propName of propNames) {
      // Security: Skip any forbidden keys that might have slipped through
      if (FORBIDDEN_KEYS.includes(propName)) {
        continue;
      }

      const propDef = (schema.properties as Record<string, unknown>)[propName];
      if (!isValidPropertyDef(propDef)) {
        continue; // Skip invalid property definitions
      }

      const prop = propDef;
      let type = 'string';
      let isVector = false;

      if (prop.type === 'integer') {
        type = 'int64';
      } else if (prop.type === 'number') {
        type = 'float64';
      } else if (prop.type === 'boolean') {
        type = 'bool';
      } else if (prop.type === 'string') {
        type = 'string';
      } else if (prop.type === 'array') {
        isVector = true;
        type = jsonTypeToFB(prop.items?.type || 'string');
      } else if (prop.type === 'object') {
        // Nested object - will be stored as BLOB (serialized FlatBuffer)
        type = propName;
      }

      // Handle enums
      if (prop.enum && Array.isArray(prop.enum)) {
        const enumValues = prop.enum.filter((v): v is string => typeof v === 'string');
        result.enums.set(propName + 'Enum', enumValues);
        type = propName + 'Enum';
      }

      fields.push({
        name: propName,
        type,
        isVector,
        defaultValue: prop.default,
        attributes: prop.required ? ['required'] : [],
      });
    }

    result.tables.push({
      name: tableName,
      namespace: '',
      fields,
      isStruct: false,
    });
    result.rootType = tableName;
  }

  // Handle definitions (sub-schemas)
  const defs = schema.definitions || schema.$defs;
  if (defs && typeof defs === 'object') {
    const defNames = Object.keys(defs);
    for (const defName of defNames) {
      // Security: Skip forbidden keys
      if (FORBIDDEN_KEYS.includes(defName)) {
        continue;
      }

      const defSchema = (defs as Record<string, unknown>)[defName];
      if (!isValidJSONSchema(defSchema)) {
        continue;
      }

      const def = defSchema;
      if (def.type === 'object' && def.properties) {
        const fields: FBField[] = [];

        const defPropNames = Object.keys(def.properties);
        for (const propName of defPropNames) {
          if (FORBIDDEN_KEYS.includes(propName)) {
            continue;
          }

          const propDef = (def.properties as Record<string, unknown>)[propName];
          if (!isValidPropertyDef(propDef)) {
            continue;
          }

          const prop = propDef;
          let type = jsonTypeToFB(prop.type || 'string');
          const isVector = prop.type === 'array';

          if (isVector) {
            type = jsonTypeToFB(prop.items?.type || 'string');
          }

          fields.push({
            name: propName,
            type,
            isVector,
            defaultValue: prop.default,
          });
        }

        result.tables.push({
          name: defName,
          namespace: '',
          fields,
          isStruct: false,
        });
      }
    }
  }

  return result;
}

function jsonTypeToFB(jsonType: string): string {
  switch (jsonType) {
    case 'integer': return 'int64';
    case 'number': return 'float64';
    case 'boolean': return 'bool';
    case 'string': return 'string';
    default: return 'string';
  }
}

// Convert parsed schema to database schema
export function toDBSchema(parsed: ParsedSchema, schemaName: string, source: string): DatabaseSchema {
  const tables: TableDef[] = [];

  for (const table of parsed.tables) {
    const columns: ColumnDef[] = [];

    // Add implicit rowid/sequence column
    columns.push({
      name: '_rowid',
      sqlType: SQLColumnType.INTEGER,
      flatbufferPath: [],
      fbTypeName: 'uint64',
      nullable: false,
      isPrimaryKey: true,
      isIndexed: true,
    });

    // Add _offset column for FlatBuffer lookup
    columns.push({
      name: '_offset',
      sqlType: SQLColumnType.INTEGER,
      flatbufferPath: [],
      fbTypeName: 'uint64',
      nullable: false,
      isPrimaryKey: false,
      isIndexed: false,
    });

    for (const field of table.fields) {
      const isRequired = field.attributes?.includes('required') ?? false;
      const isKey = field.attributes?.includes('key') ?? false;
      const isIndexed = field.attributes?.includes('indexed') ?? isKey;

      // Determine SQL type
      let sqlType: SQLColumnType;
      if (field.isVector) {
        sqlType = SQLColumnType.BLOB;
      } else if (parsed.enums.has(field.type)) {
        sqlType = SQLColumnType.INTEGER; // Enums stored as integers
      } else {
        sqlType = fbTypeToSQL(field.type);
      }

      columns.push({
        name: field.name,
        sqlType,
        flatbufferPath: [field.name],
        fbTypeName: field.type,
        nullable: !isRequired && !table.isStruct,
        defaultValue: field.defaultValue,
        isPrimaryKey: isKey,
        isIndexed,
      });
    }

    tables.push({
      name: table.name,
      fbTableName: table.name,
      fbNamespace: table.namespace,
      columns,
      primaryKey: ['_rowid'],
      indexes: columns.filter(c => c.isIndexed && c.name !== '_rowid').map(c => c.name),
    });
  }

  return {
    name: schemaName,
    tables,
    fbSchemaSource: source,
    version: 1,
  };
}

// Auto-detect and parse schema
export function parseSchema(source: string, name: string = 'default'): DatabaseSchema {
  // Security: Check size limit for all schema types
  if (source.length > MAX_SCHEMA_SIZE) {
    throw new Error(`Schema source exceeds maximum size of ${MAX_SCHEMA_SIZE} bytes`);
  }

  const trimmed = source.trim();

  // Try to detect if it's JSON
  if (trimmed.startsWith('{')) {
    const parsed = parseJSONSchema(source);
    return toDBSchema(parsed, name, source);
  }

  // Assume FlatBuffer IDL
  const parsed = parseFlatBufferIDL(source);
  return toDBSchema(parsed, name, source);
}
