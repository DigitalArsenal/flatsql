import { parseSchema } from '../src/schema/index.js';

describe('JSON schema join tables', () => {
  test('creates join table for referenced definitions', () => {
    const jsonSchema = JSON.stringify({
      $schema: 'http://json-schema.org/draft-07/schema#',
      title: 'Satellite',
      type: 'object',
      properties: {
        name: { type: 'string' },
        orbit: { $ref: '#/$defs/Orbit' },
      },
      $defs: {
        Orbit: {
          type: 'object',
          properties: {
            perigee: { type: 'number' },
            apogee: { type: 'number' },
          },
        },
      },
    });

    const schema = parseSchema(jsonSchema, 'satellite');
    const tableNames = schema.tables.map((table) => table.name);

    expect(tableNames).toEqual(
      expect.arrayContaining(['Satellite', 'Orbit', 'Satellite_Orbit_join']),
    );

    const joinTable = schema.tables.find((table) => table.name === 'Satellite_Orbit_join');
    expect(joinTable).toBeDefined();
    if (joinTable) {
      const columnNames = joinTable.columns.map((col) => col.name);
      expect(columnNames).toEqual(expect.arrayContaining(['SatelliteRowId', 'OrbitRowId']));
    }
  });
});
