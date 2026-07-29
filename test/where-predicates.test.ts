// WHERE-clause grammar and the fail-closed contract.
//
// Regression: an unparsed predicate (LIKE, IN, AND/OR, ...) used to leave
// `where` undefined, and the query fell through to `scanAll()`. A caller asking
// for one satellite got the entire 32k-object catalogue back with no error and
// no way to detect it. An unsupported predicate must THROW.

import { parseSchema } from '../src/schema/index.js';
import { FlatSQLDatabase } from '../src/index.js';

class MockAccessor {
  getField(data: Uint8Array, path: string[]): any {
    let obj = JSON.parse(new TextDecoder().decode(data));
    for (const key of path) {
      if (obj === null || obj === undefined) return null;
      obj = obj[key];
    }
    return obj;
  }

  buildBuffer(_tableName: string, fields: Record<string, any>): Uint8Array {
    return new TextEncoder().encode(JSON.stringify(fields));
  }
}

const SAT_SCHEMA = `
  table Sat {
    norad_cat_id: int;
    object_id: string;
    name: string;
    apogee: double;
  }
  root_type Sat;
`;

const FIXTURES = [
  { norad_cat_id: 25544, object_id: '1998-067A', name: 'ISS (ZARYA)', apogee: 421.0 },
  { norad_cat_id: 20580, object_id: '1990-037B', name: 'HST', apogee: 540.5 },
  { norad_cat_id: 48274, object_id: '2021-035A', name: 'TIANHE', apogee: 380.2 },
  { norad_cat_id: 44713, object_id: '2019-074A', name: 'STARLINK-1007', apogee: 550.0 },
  { norad_cat_id: 44714, object_id: '2019-074B', name: 'STARLINK-1008', apogee: 551.0 },
  { norad_cat_id: 900001, object_id: '2020-001A', name: null, apogee: null },
];

function createDb() {
  const db = new FlatSQLDatabase(parseSchema(SAT_SCHEMA, 'sats'), new MockAccessor());
  for (const row of FIXTURES) {
    db.insert('Sat', row);
  }
  return db;
}

const namesOf = (result: { columns: string[]; rows: any[][] }) => {
  const column = result.columns.indexOf('name');
  return result.rows.map(row => row[column]);
};

describe('WHERE fail-closed contract', () => {
  test('an unsupported predicate throws instead of returning every row', () => {
    const db = createDb();

    // The exact shapes measured returning the whole table on the live engine.
    const poison = [
      'SELECT * FROM Sat WHERE name REGEXP ".*"',
      'SELECT * FROM Sat WHERE norad_cat_id',
      'SELECT * FROM Sat WHERE name = ',
      'SELECT * FROM Sat WHERE (name = 1',
      'SELECT * FROM Sat WHERE name GLOB 1',
      'SELECT * FROM Sat WHERE name = other_column',
    ];

    for (const sql of poison) {
      expect(() => db.query(sql)).toThrow(/SQL parse error/);
    }
  });

  test('a failed parse never degrades into a full scan', () => {
    const db = createDb();
    let rowCount: number | undefined;

    try {
      rowCount = db.query('SELECT * FROM Sat WHERE name MATCHES 1').rowCount;
    } catch {
      rowCount = undefined;
    }

    // The whole point: no result at all, rather than all six rows.
    expect(rowCount).toBeUndefined();
    expect(db.query('SELECT * FROM Sat').rowCount).toBe(FIXTURES.length);
  });

  test('an unsupported ORDER BY or LIMIT throws', () => {
    const db = createDb();
    expect(() => db.query('SELECT * FROM Sat ORDER BY apogee * 2')).toThrow(/SQL parse error/);
    expect(() => db.query('SELECT * FROM Sat LIMIT all')).toThrow(/SQL parse error/);
  });

  test('errors never echo literal values back to the caller', () => {
    const db = createDb();
    expect(() => db.query("SELECT * FROM Sat WHERE name = 'hunter2' 'hunter2'")).toThrow(
      /string literal/
    );
    expect(() => db.query("SELECT * FROM Sat WHERE name = 'hunter2' 'hunter2'")).not.toThrow(
      /hunter2/
    );
  });
});

describe('LIKE', () => {
  test('matches substrings case-insensitively rather than returning the table', () => {
    const db = createDb();
    const result = db.query("SELECT name FROM Sat WHERE name LIKE '%zarya%'");
    expect(result.rowCount).toBe(1);
    expect(namesOf(result)).toEqual(['ISS (ZARYA)']);
  });

  test('prefix patterns', () => {
    const db = createDb();
    expect(
      db.query("SELECT name FROM Sat WHERE object_id LIKE '1998-067%'").rowCount
    ).toBe(1);
    expect(
      db.query("SELECT name FROM Sat WHERE name LIKE 'STARLINK%'").rowCount
    ).toBe(2);
  });

  test('_ matches exactly one character', () => {
    const db = createDb();
    expect(db.query("SELECT name FROM Sat WHERE object_id LIKE '2019-074_'").rowCount).toBe(2);
    expect(db.query("SELECT name FROM Sat WHERE object_id LIKE '2019-074_A'").rowCount).toBe(0);
  });

  test('regex metacharacters in a pattern are literal, not injected', () => {
    const db = createDb();
    // '.' must not behave as "any character".
    expect(db.query("SELECT name FROM Sat WHERE name LIKE 'HS.'").rowCount).toBe(0);
    expect(db.query("SELECT name FROM Sat WHERE name LIKE 'ISS (ZARYA)'").rowCount).toBe(1);
  });

  test('NOT LIKE excludes matches and rows with no value', () => {
    const db = createDb();
    const result = db.query("SELECT name FROM Sat WHERE name NOT LIKE 'STARLINK%'");
    expect(namesOf(result).sort()).toEqual(['HST', 'ISS (ZARYA)', 'TIANHE']);
  });

  test('a wildcard-only pattern still matches every non-null row', () => {
    const db = createDb();
    expect(db.query("SELECT name FROM Sat WHERE name LIKE '%%%'").rowCount).toBe(5);
  });
});

describe('IN', () => {
  test('returns only the listed values', () => {
    const db = createDb();
    const result = db.query('SELECT name FROM Sat WHERE norad_cat_id IN (25544, 20580, 48274)');
    expect(result.rowCount).toBe(3);
    expect(namesOf(result).sort()).toEqual(['HST', 'ISS (ZARYA)', 'TIANHE']);
  });

  test('string lists', () => {
    const db = createDb();
    expect(
      db.query("SELECT name FROM Sat WHERE object_id IN ('1998-067A', '1990-037B')").rowCount
    ).toBe(2);
  });

  test('NOT IN', () => {
    const db = createDb();
    expect(db.query('SELECT name FROM Sat WHERE norad_cat_id NOT IN (25544)').rowCount).toBe(5);
  });

  test('an empty list matches nothing', () => {
    const db = createDb();
    expect(db.query('SELECT name FROM Sat WHERE norad_cat_id IN ()').rowCount).toBe(0);
  });
});

describe('AND / OR / NOT / parentheses', () => {
  test('AND intersects', () => {
    const db = createDb();
    const result = db.query(
      "SELECT name FROM Sat WHERE name LIKE 'STARLINK%' AND norad_cat_id = 44713"
    );
    expect(namesOf(result)).toEqual(['STARLINK-1007']);
  });

  test('OR unions', () => {
    const db = createDb();
    const result = db.query(
      'SELECT name FROM Sat WHERE norad_cat_id = 25544 OR norad_cat_id = 20580'
    );
    expect(namesOf(result).sort()).toEqual(['HST', 'ISS (ZARYA)']);
  });

  test('AND binds tighter than OR', () => {
    const db = createDb();
    // ISS matches the LIKE; HST only qualifies through the second branch.
    const result = db.query(
      "SELECT name FROM Sat WHERE name LIKE '%ZARYA%' AND apogee > 400 OR norad_cat_id = 20580"
    );
    expect(namesOf(result).sort()).toEqual(['HST', 'ISS (ZARYA)']);
  });

  test('parentheses override precedence', () => {
    const db = createDb();
    const result = db.query(
      "SELECT name FROM Sat WHERE name LIKE '%ZARYA%' AND (apogee > 900 OR norad_cat_id = 20580)"
    );
    expect(result.rowCount).toBe(0);
  });

  test('NOT negates a group', () => {
    const db = createDb();
    const result = db.query(
      'SELECT name FROM Sat WHERE NOT (norad_cat_id = 25544 OR norad_cat_id = 20580)'
    );
    expect(result.rowCount).toBe(4);
  });

  test('BETWEEN composes with AND without swallowing the conjunction', () => {
    const db = createDb();
    const result = db.query(
      "SELECT name FROM Sat WHERE apogee BETWEEN 400 AND 600 AND name LIKE 'STARLINK%'"
    );
    expect(namesOf(result).sort()).toEqual(['STARLINK-1007', 'STARLINK-1008']);
  });
});

describe('IS NULL', () => {
  test('finds and excludes rows with no value', () => {
    const db = createDb();
    expect(db.query('SELECT name FROM Sat WHERE name IS NULL').rowCount).toBe(1);
    expect(db.query('SELECT name FROM Sat WHERE name IS NOT NULL').rowCount).toBe(5);
  });

  test('comparisons never match a null field', () => {
    const db = createDb();
    expect(db.query("SELECT name FROM Sat WHERE name != 'HST'").rowCount).toBe(4);
  });
});

describe('ORDER BY', () => {
  test('sorts ascending and descending instead of being ignored', () => {
    const db = createDb();
    expect(
      db.query('SELECT norad_cat_id FROM Sat ORDER BY norad_cat_id').rows.map(r => r[0])
    ).toEqual([20580, 25544, 44713, 44714, 48274, 900001]);

    expect(
      db.query('SELECT norad_cat_id FROM Sat ORDER BY norad_cat_id DESC').rows.map(r => r[0])
    ).toEqual([900001, 48274, 44714, 44713, 25544, 20580]);
  });

  test('ORDER BY applies before LIMIT', () => {
    const db = createDb();
    const result = db.query('SELECT norad_cat_id FROM Sat ORDER BY norad_cat_id DESC LIMIT 2');
    expect(result.rows.map(r => r[0])).toEqual([900001, 48274]);
  });

  test('sorts by multiple terms', () => {
    const db = createDb();
    const result = db.query(
      "SELECT object_id FROM Sat WHERE name LIKE 'STARLINK%' ORDER BY apogee DESC, object_id"
    );
    expect(result.rows.map(r => r[0])).toEqual(['2019-074B', '2019-074A']);
  });
});

describe('literal handling', () => {
  test('quoted strings survive clause splitting', () => {
    const db = createDb();
    // 'ORDER BY' and 'LIMIT 1' inside a literal are not clause boundaries.
    expect(db.query("SELECT name FROM Sat WHERE name = 'ORDER BY'").rowCount).toBe(0);
    expect(db.query("SELECT name FROM Sat WHERE name LIKE '%LIMIT 1%'").rowCount).toBe(0);
  });

  test('doubled quotes escape inside a literal', () => {
    const db = new FlatSQLDatabase(parseSchema(SAT_SCHEMA, 'sats'), new MockAccessor());
    db.insert('Sat', { norad_cat_id: 1, object_id: 'x', name: "O'BRIEN", apogee: 1 });
    expect(db.query("SELECT name FROM Sat WHERE name = 'O''BRIEN'").rowCount).toBe(1);
  });

  test('indexed equality still uses the index fast path', () => {
    const db = createDb();
    db.createIndex('Sat', 'norad_cat_id');
    expect(db.query('SELECT name FROM Sat WHERE norad_cat_id = 25544').rowCount).toBe(1);
    // ...and an indexed conjunct does not drop the rest of the predicate.
    expect(
      db.query("SELECT name FROM Sat WHERE norad_cat_id = 25544 AND name LIKE '%HST%'").rowCount
    ).toBe(0);
  });
});
