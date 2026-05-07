declare module 'node:sqlite' {
  export interface SQLiteColumnMetadata {
    column: string;
    database: string;
    name: string;
    table: string;
    type: string | null;
  }

  export interface StatementSync {
    all(...params: any[]): Record<string, unknown>[];
    run(...params: any[]): unknown;
    columns(): SQLiteColumnMetadata[];
  }

  export class DatabaseSync {
    constructor(path: string);
    exec(sql: string): void;
    prepare(sql: string): StatementSync;
    close(): void;
  }
}
