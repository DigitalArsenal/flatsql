const decoder = new TextDecoder();

export interface ArtifactRecordExtractor {
  getField(data: Uint8Array, fieldName: string): unknown;
  getFields?(data: Uint8Array, fieldNames: string[]): Record<string, unknown>;
}

export type ArtifactFieldExtractor = ((data: Uint8Array, fieldName: string) => unknown) | ArtifactRecordExtractor;

interface FlatBufferCursor {
  readonly data: Uint8Array;
  readonly view: DataView;
  readonly root: number;
  readonly vtable: number;
  readonly vtableSize: number;
}

function createCursor(data: Uint8Array): FlatBufferCursor {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const root = view.getUint32(0, true);
  const vtable = root - view.getInt32(root, true);
  return {
    data,
    view,
    root,
    vtable,
    vtableSize: view.getUint16(vtable, true),
  };
}

function getFieldOffset(cursor: FlatBufferCursor, fieldIndex: number): number {
  const entryOffset = cursor.vtable + 4 + fieldIndex * 2;
  if (entryOffset + 2 > cursor.vtable + cursor.vtableSize) {
    return 0;
  }
  return cursor.view.getUint16(entryOffset, true);
}

function readInt32Field(cursor: FlatBufferCursor, fieldIndex: number): number {
  const fieldOffset = getFieldOffset(cursor, fieldIndex);
  return fieldOffset === 0 ? 0 : cursor.view.getInt32(cursor.root + fieldOffset, true);
}

function readStringField(cursor: FlatBufferCursor, fieldIndex: number): string {
  const fieldOffset = getFieldOffset(cursor, fieldIndex);
  if (fieldOffset === 0) {
    return '';
  }

  const relative = cursor.view.getUint32(cursor.root + fieldOffset, true);
  const stringStart = cursor.root + fieldOffset + relative;
  const stringLength = cursor.view.getUint32(stringStart, true);
  return decoder.decode(cursor.data.subarray(stringStart + 4, stringStart + 4 + stringLength));
}

function createMappedExtractor(fieldReaders: Record<string, (cursor: FlatBufferCursor) => unknown>): ArtifactRecordExtractor {
  return {
    getField(data: Uint8Array, fieldName: string): unknown {
      const reader = fieldReaders[fieldName];
      if (!reader) {
        return null;
      }
      return reader(createCursor(data));
    },
    getFields(data: Uint8Array, fieldNames: string[]): Record<string, unknown> {
      const cursor = createCursor(data);
      return Object.fromEntries(
        fieldNames.map((fieldName) => {
          const reader = fieldReaders[fieldName];
          return [fieldName, reader ? reader(cursor) : null];
        })
      );
    },
  };
}

export function extractArtifactField(extractor: ArtifactFieldExtractor, data: Uint8Array, fieldName: string): unknown {
  return typeof extractor === 'function' ? extractor(data, fieldName) : extractor.getField(data, fieldName);
}

export function extractArtifactFields(
  extractor: ArtifactFieldExtractor,
  data: Uint8Array,
  fieldNames: string[]
): Record<string, unknown> {
  if (typeof extractor !== 'function' && extractor.getFields) {
    return extractor.getFields(data, fieldNames);
  }

  return Object.fromEntries(fieldNames.map((fieldName) => [fieldName, extractArtifactField(extractor, data, fieldName)]));
}

export const demoExtractors: Record<string, ArtifactFieldExtractor> = {
  User: createMappedExtractor({
    id: (cursor) => readInt32Field(cursor, 0),
    name: (cursor) => readStringField(cursor, 1),
    email: (cursor) => readStringField(cursor, 2),
    age: (cursor) => readInt32Field(cursor, 3),
  }),
  Post: createMappedExtractor({
    id: (cursor) => readInt32Field(cursor, 0),
    user_id: (cursor) => readInt32Field(cursor, 1),
    title: (cursor) => readStringField(cursor, 2),
  }),
};
