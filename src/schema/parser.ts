// Schema parser for FlatBuffers IDL and JSON Schema
// Converts to internal table definitions

import { ColumnDef, TableDef, DatabaseSchema, SQLColumnType, fbTypeToSQL } from './types.js';

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

// Parse JSON Schema
export function parseJSONSchema(source: string): ParsedSchema {
  const schema = JSON.parse(source);
  const result: ParsedSchema = {
    tables: [],
    enums: new Map(),
  };

  // Handle top-level object as a table
  if (schema.type === 'object' && schema.properties) {
    const tableName = schema.title || 'Root';
    const fields: FBField[] = [];

    for (const [propName, propDef] of Object.entries(schema.properties)) {
      const prop = propDef as any;
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
      if (prop.enum) {
        result.enums.set(propName + 'Enum', prop.enum);
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
  if (schema.definitions || schema.$defs) {
    const defs = schema.definitions || schema.$defs;
    for (const [defName, defSchema] of Object.entries(defs)) {
      const def = defSchema as any;
      if (def.type === 'object' && def.properties) {
        const fields: FBField[] = [];

        for (const [propName, propDef] of Object.entries(def.properties)) {
          const prop = propDef as any;
          let type = jsonTypeToFB(prop.type);
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
