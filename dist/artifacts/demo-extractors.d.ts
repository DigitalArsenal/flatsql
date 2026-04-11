export interface ArtifactRecordExtractor {
    getField(data: Uint8Array, fieldName: string): unknown;
    compileFieldValues?(fieldNames: string[]): (data: Uint8Array) => unknown[];
    getFieldValues?(data: Uint8Array, fieldNames: string[]): unknown[];
    getFields?(data: Uint8Array, fieldNames: string[]): Record<string, unknown>;
}
export type ArtifactFieldExtractor = ((data: Uint8Array, fieldName: string) => unknown) | ArtifactRecordExtractor;
export declare function extractArtifactField(extractor: ArtifactFieldExtractor, data: Uint8Array, fieldName: string): unknown;
export declare function extractArtifactFields(extractor: ArtifactFieldExtractor, data: Uint8Array, fieldNames: string[]): Record<string, unknown>;
export declare function extractArtifactFieldValues(extractor: ArtifactFieldExtractor, data: Uint8Array, fieldNames: string[]): unknown[];
export declare function createArtifactFieldValueReader(extractor: ArtifactFieldExtractor, fieldNames: string[]): (data: Uint8Array) => unknown[];
export declare const demoExtractors: Record<string, ArtifactFieldExtractor>;
//# sourceMappingURL=demo-extractors.d.ts.map