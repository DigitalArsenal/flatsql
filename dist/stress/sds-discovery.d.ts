import type { SdsSchemaInfo } from './types.js';
export declare function parseSchemaMetadata(name: string, source: string, filePath: string): SdsSchemaInfo;
export declare function discoverSdsSchemas(schemaRoot: string): Promise<SdsSchemaInfo[]>;
//# sourceMappingURL=sds-discovery.d.ts.map