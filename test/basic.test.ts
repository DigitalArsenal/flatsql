// Basic tests for FlatBuffers-SQLite

import { BTree, KeyType } from '../src/btree/index.js';
import { StackedFlatBufferStore } from '../src/storage/index.js';
import { parseSchema } from '../src/schema/index.js';

describe('BTree', () => {
  test('insert and search', () => {
    const tree = new BTree({
      name: 'test_idx',
      tableName: 'test',
      columnName: 'id',
      keyType: KeyType.Int,
      order: 4,
    });

    // Insert some entries
    tree.insert(5, 100n, 50, 1n);
    tree.insert(3, 200n, 50, 2n);
    tree.insert(7, 300n, 50, 3n);
    tree.insert(1, 400n, 50, 4n);
    tree.insert(9, 500n, 50, 5n);

    // Search
    const results = tree.search(3);
    expect(results.length).toBe(1);
    expect(results[0].dataOffset).toBe(200n);
    expect(results[0].sequence).toBe(2n);
  });

  test('range query', () => {
    const tree = new BTree({
      name: 'test_idx',
      tableName: 'test',
      columnName: 'value',
      keyType: KeyType.Int,
      order: 4,
    });

    for (let i = 0; i < 20; i++) {
      tree.insert(i, BigInt(i * 100), 50, BigInt(i));
    }

    const results = tree.range(5, 10);
    expect(results.length).toBe(6); // 5, 6, 7, 8, 9, 10
  });

  test('string keys', () => {
    const tree = new BTree({
      name: 'name_idx',
      tableName: 'users',
      columnName: 'name',
      keyType: KeyType.String,
      order: 4,
    });

    tree.insert('alice', 100n, 50, 1n);
    tree.insert('bob', 200n, 50, 2n);
    tree.insert('charlie', 300n, 50, 3n);

    const results = tree.search('bob');
    expect(results.length).toBe(1);
    expect(results[0].dataOffset).toBe(200n);
  });

  test('returns every duplicate key after node splits', () => {
    const tree = new BTree({
      name: 'file_id_idx',
      tableName: 'records',
      columnName: 'FILE_ID',
      keyType: KeyType.String,
      order: 4,
    });

    for (let index = 0; index < 20; index++) {
      tree.insert('publish-event-1', BigInt(index * 100), 50, BigInt(index));
    }

    const results = tree.search('publish-event-1');

    expect(results).toHaveLength(20);
    expect(new Set(results.map((entry) => entry.sequence))).toHaveProperty('size', 20);
  });

  test('serialization', () => {
    const tree = new BTree({
      name: 'test_idx',
      tableName: 'test',
      columnName: 'id',
      keyType: KeyType.Int,
      order: 4,
    });

    tree.insert(1, 100n, 50, 1n);
    tree.insert(2, 200n, 50, 2n);
    tree.insert(3, 300n, 50, 3n);

    const serialized = tree.serialize();
    const restored = BTree.deserialize(serialized);

    const results = restored.search(2);
    expect(results.length).toBe(1);
    expect(results[0].dataOffset).toBe(200n);
  });
});

describe('StackedFlatBufferStore', () => {
  test('append and read', () => {
    const store = new StackedFlatBufferStore('test_schema');

    const data1 = new Uint8Array([1, 2, 3, 4, 5]);
    const data2 = new Uint8Array([6, 7, 8, 9, 10]);

    const offset1 = store.append('table1', data1);
    const offset2 = store.append('table1', data2);

    expect(offset1).not.toBe(offset2);

    const record1 = store.readRecord(offset1);
    expect(record1.data).toEqual(data1);
    expect(record1.header.tableName).toBe('table1');

    const record2 = store.readRecord(offset2);
    expect(record2.data).toEqual(data2);
  });

  test('iterate records', () => {
    const store = new StackedFlatBufferStore('test_schema');

    store.append('table1', new Uint8Array([1, 2, 3]));
    store.append('table2', new Uint8Array([4, 5, 6]));
    store.append('table1', new Uint8Array([7, 8, 9]));

    const allRecords = Array.from(store.iterateRecords());
    expect(allRecords.length).toBe(3);

    const table1Records = Array.from(store.iterateTableRecords('table1'));
    expect(table1Records.length).toBe(2);
  });

  test('export and restore', () => {
    const store = new StackedFlatBufferStore('test_schema');

    store.append('table1', new Uint8Array([1, 2, 3, 4, 5]));
    store.append('table1', new Uint8Array([6, 7, 8, 9, 10]));

    const exported = store.getData();
    const restored = StackedFlatBufferStore.fromData(exported);

    expect(restored.getRecordCount()).toBe(2n);
    expect(restored.getSchemaName()).toBe('test_schema');
  });
});

describe('Schema Parser', () => {
  test('parse FlatBuffer IDL', () => {
    const idl = `
      namespace Game;

      enum Color : byte { Red, Green, Blue }

      table Monster {
        name: string;
        hp: int = 100;
        color: Color;
      }

      root_type Monster;
    `;

    const schema = parseSchema(idl, 'game');

    expect(schema.name).toBe('game');
    expect(schema.tables.length).toBe(1);
    expect(schema.tables[0].name).toBe('Monster');
    expect(schema.tables[0].fbNamespace).toBe('Game');

    const columns = schema.tables[0].columns;
    expect(columns.some(c => c.name === 'name')).toBe(true);
    expect(columns.some(c => c.name === 'hp')).toBe(true);
    expect(columns.some(c => c.name === 'color')).toBe(true);
  });

  test('parse JSON Schema', () => {
    const jsonSchema = `{
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "Person",
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "age": { "type": "integer" },
        "active": { "type": "boolean" }
      },
      "required": ["name"]
    }`;

    const schema = parseSchema(jsonSchema, 'person');

    expect(schema.name).toBe('person');
    expect(schema.tables.length).toBe(1);
    expect(schema.tables[0].name).toBe('Person');

    const columns = schema.tables[0].columns;
    expect(columns.some(c => c.name === 'name')).toBe(true);
    expect(columns.some(c => c.name === 'age')).toBe(true);
  });
});
