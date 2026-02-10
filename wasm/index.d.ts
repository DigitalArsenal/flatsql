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

  // ==================== Raw FlatBuffer Access ====================

  /**
   * Get raw FlatBuffer pointer and size by table name and id.
   * Returns { ptr, size, sequence } or null if not found.
   * Use the ptr with Module.HEAPU8.subarray(ptr, ptr + size) to access raw bytes.
   */
  getFlatBufferById(tableName: string, id: number): { ptr: number; size: number; sequence: number } | null;

  /**
   * Get raw FlatBuffer pointer and size by table name and email.
   * Returns { ptr, size, sequence } or null if not found.
   */
  getFlatBufferByEmail(tableName: string, email: string): { ptr: number; size: number; sequence: number } | null;

  /**
   * Get a copy of the raw FlatBuffer data as Uint8Array
   */
  getFlatBufferDataById(tableName: string, id: number): Uint8Array | null;

  /**
   * Get the underlying storage buffer info
   * Returns { ptr, size } for direct WASM memory access
   */
  getStorageInfo(): { ptr: number; size: number };

  /**
   * Destroy the database and free memory
   */
  destroy(): void;

  // ==================== Encryption ====================

  /**
   * Set the encryption key for field-level FlatBuffer decryption.
   * Fields marked with (encrypted) in the schema will be transparently
   * decrypted when read through SQL queries.
   * @param key 32-byte AES-256 key
   */
  setEncryptionKey(key: Uint8Array): void;

  /**
   * Check if encryption is enabled
   */
  isEncrypted(): boolean;

  /**
   * Encrypt a FlatBuffer using the database's encryption key
   * @param buffer FlatBuffer data
   * @param schema Binary schema (.bfbs)
   * @returns Encrypted buffer copy
   */
  encryptBuffer(buffer: Uint8Array, schema: Uint8Array): Uint8Array;

  /**
   * Decrypt a FlatBuffer using the database's encryption key
   * @param buffer Encrypted FlatBuffer data
   * @param schema Binary schema (.bfbs)
   * @returns Decrypted buffer copy
   */
  decryptBuffer(buffer: Uint8Array, schema: Uint8Array): Uint8Array;

  // ==================== HMAC Authentication ====================

  /**
   * Enable or disable HMAC-SHA256 verification on ingest.
   * When enabled, buffers can be verified for integrity before processing.
   * Requires an encryption key to be set first.
   * @param enabled true to enable, false to disable
   */
  setHMACVerification(enabled: boolean): void;

  /**
   * Check if HMAC verification is enabled
   */
  isHMACEnabled(): boolean;

  /**
   * Compute HMAC-SHA256 for a FlatBuffer.
   * Use this to sign buffers before transmission/storage.
   * @param buffer FlatBuffer data
   * @returns 32-byte HMAC
   */
  computeHMAC(buffer: Uint8Array): Uint8Array;

  /**
   * Verify HMAC-SHA256 for a FlatBuffer.
   * Use this to verify buffer integrity before ingestion.
   * @param buffer FlatBuffer data
   * @param mac 32-byte HMAC to verify against
   * @returns true if the MAC is valid
   */
  verifyHMAC(buffer: Uint8Array, mac: Uint8Array): boolean;
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

  /**
   * Check if WASM was loaded with integrity verification
   */
  wasIntegrityVerified(): boolean;
}

/**
 * Options for initializing FlatSQL with integrity verification
 */
export interface InitOptions {
  /**
   * Expected SHA-384 hash of the WASM binary (base64 encoded)
   * If provided, the WASM will be verified against this hash before loading
   */
  integrity?: string;

  /**
   * Custom path to WASM files directory
   * Default: same directory as index.js
   */
  wasmPath?: string;

  /**
   * Skip integrity verification (not recommended for production)
   * Default: false
   */
  skipIntegrityCheck?: boolean;

  /**
   * Fail if integrity cannot be verified (no hash available)
   * Default: false
   */
  requireIntegrity?: boolean;

  /**
   * Custom Emscripten module factory function
   * Default: built-in FlatSQLModule
   */
  moduleFactory?: () => Promise<any>;
}

/**
 * Initialize FlatSQL WASM module with optional integrity verification
 *
 * @example
 * // Basic usage (auto-loads integrity.json if available)
 * const flatsql = await initFlatSQL();
 *
 * @example
 * // With explicit integrity hash
 * const flatsql = await initFlatSQL({
 *   integrity: 'base64-hash-here',
 *   requireIntegrity: true
 * });
 *
 * @example
 * // Skip integrity check (development only)
 * const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
 *
 * @example
 * // Legacy API (still supported)
 * const flatsql = await initFlatSQL(customModuleFactory);
 */
export function initFlatSQL(options?: InitOptions | (() => Promise<any>)): Promise<FlatSQL>;

/**
 * Check if WASM was loaded with integrity verification
 * Can be called before or after initFlatSQL
 */
export function wasIntegrityVerified(): boolean;

export default initFlatSQL;
