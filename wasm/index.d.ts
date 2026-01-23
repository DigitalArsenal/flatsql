/**
 * FlatSQL WASM Module
 * SQL queries over raw FlatBuffer storage
 */

export interface QueryResult {
  columns: string[];
  rows: any[][];
}

export interface TableStats {
  tableName: string;
  fileId: string;
  recordCount: number;
}

export interface FlatSQLDatabase {
  /**
   * Register a file identifier to route FlatBuffers to a table
   */
  registerFileId(fileId: string, tableName: string): void;

  /**
   * Enable demo field extractors for User and Post tables
   */
  enableDemoExtractors(): void;

  /**
   * Ingest a stream of size-prefixed FlatBuffers
   * Format: [4-byte size LE][FlatBuffer][4-byte size LE][FlatBuffer]...
   * @param data Stream of size-prefixed FlatBuffers
   * @param source Optional source name (requires registerSource() first)
   */
  ingest(data: Uint8Array, source?: string | null): number;

  /**
   * Ingest a single FlatBuffer (without size prefix)
   * @param data FlatBuffer data
   * @param source Optional source name (requires registerSource() first)
   */
  ingestOne(data: Uint8Array, source?: string | null): number;

  /**
   * Execute a SQL query
   */
  query(sql: string): QueryResult;

  /**
   * Export all data as a stream of size-prefixed FlatBuffers
   */
  exportData(): Uint8Array;

  /**
   * Load exported data and rebuild indexes
   */
  loadAndRebuild(data: Uint8Array): void;

  /**
   * Get statistics for all tables
   */
  getStats(): TableStats[];

  // ==================== Multi-Source API ====================

  /**
   * Register a named data source for source-aware ingestion.
   * Creates source-specific tables: User@siteA, Post@siteA, etc.
   *
   * @example
   * db.registerSource('siteA');
   * db.registerSource('siteB');
   * db.createUnifiedViews();
   *
   * // Now ingest to specific sources
   * db.ingest(streamA, 'siteA');
   * db.ingest(streamB, 'siteB');
   *
   * // Query specific source
   * db.query('SELECT * FROM "User@siteA"');
   *
   * // Query all sources (unified view)
   * db.query('SELECT * FROM User');
   */
  registerSource(sourceName: string): void;

  /**
   * Create unified views for cross-source queries.
   * Call this after registering all sources and file IDs.
   * Creates views like "User" that combine User@siteA, User@siteB, etc.
   */
  createUnifiedViews(): void;

  /**
   * List registered source names
   */
  listSources(): string[];

  // ==================== Delete Support ====================

  /**
   * Mark a record as deleted by table name and row ID
   */
  markDeleted(tableName: string, rowId: number): void;

  /**
   * Get count of deleted records in a table
   */
  getDeletedCount(tableName: string): number;

  /**
   * Clear tombstones (deleted record markers) for a table
   */
  clearTombstones(tableName: string): void;

  /**
   * Destroy the database and free memory
   */
  destroy(): void;
}

export interface FlatSQL {
  /**
   * Create a new database with the given schema
   */
  createDatabase(schema: string, name: string): FlatSQLDatabase;

  /**
   * Create a test User FlatBuffer (for demos)
   */
  createTestUser(id: number, name: string, email: string, age: number): Uint8Array;

  /**
   * Create a test Post FlatBuffer (for demos)
   */
  createTestPost(id: number, userId: number, title: string): Uint8Array;
}

/**
 * Initialize FlatSQL WASM module
 */
export function initFlatSQL(moduleFactory?: any): Promise<FlatSQL>;

export default initFlatSQL;
