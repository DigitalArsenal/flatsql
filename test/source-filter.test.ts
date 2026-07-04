import initFlatSQL from '../wasm/index.js';

// Regression: `WHERE _source = '<Table>@<source>'` used to return rows from
// EVERY source partition — xBestIndex claimed the equality constraint with
// omit=1 while xFilter ignored the argv. Virtual-column constraints are now
// left to SQLite (evaluated against the xColumn value).
describe('unified-view _source filtering', () => {
  const USER_SCHEMA = `
    table User {
      id: int (id);
      name: string;
      email: string (key);
      age: int;
    }
  `;

  async function createTwoSourceDb(name: string) {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, name);
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.registerSource('siteA');
    db.registerSource('siteB');
    db.createUnifiedViews();
    db.ingestBuffers([flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30)], 'siteA');
    db.ingestBuffers([flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25)], 'siteB');
    return db;
  }

  test('literal _source equality returns only the matching partition', async () => {
    const db = await createTwoSourceDb('source-filter-literal');
    expect(db.query("SELECT name, _source FROM User WHERE _source = 'User@siteB'")).toEqual({
      columns: ['name', '_source'],
      rows: [['Bob', 'User@siteB']],
    });
    expect(db.query("SELECT COUNT(*) FROM User WHERE _source = 'User@siteA'").rows).toEqual([[1]]);
    expect(db.query("SELECT COUNT(*) FROM User WHERE _source = 'User@missing'").rows).toEqual([[0]]);
    db.destroy();
  });

  test('parameterized _source equality returns only the matching partition', async () => {
    const db = await createTwoSourceDb('source-filter-params');
    expect(db.query('SELECT name FROM User WHERE _source = ?', ['User@siteA'])).toEqual({
      columns: ['name'],
      rows: [['Alice']],
    });
    // The server's epoch-profile predicate shape: '' selects all sources.
    expect(db.query("SELECT COUNT(*) FROM User WHERE (?1 = '' OR _source = ?1)", ['']).rows).toEqual([[2]]);
    expect(db.query("SELECT COUNT(*) FROM User WHERE (?1 = '' OR _source = ?1)", ['User@siteB']).rows).toEqual([[1]]);
    db.destroy();
  });

  test('direct shadow-table queries remain scoped to their partition', async () => {
    const db = await createTwoSourceDb('source-filter-shadow');
    expect(db.query('SELECT name FROM "User@siteA"').rows).toEqual([['Alice']]);
    expect(db.query('SELECT name FROM "User@siteB"').rows).toEqual([['Bob']]);
    db.destroy();
  });
});
