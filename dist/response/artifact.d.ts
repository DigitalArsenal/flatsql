import type { QueryResult } from '../core/database.js';
import type { QueryResponseArtifact, QueryResponseArtifactOptions } from './types.js';
export declare function createQueryResponseArtifact(result: QueryResult, options: QueryResponseArtifactOptions): QueryResponseArtifact;
export declare function getResponseArtifactChunk(artifact: QueryResponseArtifact, index: number): Uint8Array;
//# sourceMappingURL=artifact.d.ts.map