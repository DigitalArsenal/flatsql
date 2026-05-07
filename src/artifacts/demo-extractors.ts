const decoder = new TextDecoder();

export interface ArtifactRecordExtractor {
  getField(data: Uint8Array, fieldName: string): unknown;
  compileFieldAppender?(fieldNames: string[]): (pendingArgs: unknown[], data: Uint8Array) => void;
  compileFieldValues?(fieldNames: string[]): (data: Uint8Array) => unknown[];
  getFieldValues?(data: Uint8Array, fieldNames: string[]): unknown[];
  getFields?(data: Uint8Array, fieldNames: string[]): Record<string, unknown>;
}

export type ArtifactFieldExtractor = ((data: Uint8Array, fieldName: string) => unknown) | ArtifactRecordExtractor;

interface FlatBufferState {
  readonly data: Uint8Array;
  readonly view: DataView;
  readonly root: number;
  readonly vtable: number;
  readonly vtableSize: number;
}

interface FlatBufferFieldDescriptor {
  readonly kind: 'int32' | 'string';
  readonly index: number;
}

function createState(data: Uint8Array): FlatBufferState {
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

function getFieldOffset(view: DataView, vtable: number, vtableSize: number, fieldIndex: number): number {
  const entryOffset = vtable + 4 + fieldIndex * 2;
  if (entryOffset + 2 > vtable + vtableSize) {
    return 0;
  }
  return view.getUint16(entryOffset, true);
}

function readInt32Field(
  view: DataView,
  root: number,
  vtable: number,
  vtableSize: number,
  fieldIndex: number
): number {
  const fieldOffset = getFieldOffset(view, vtable, vtableSize, fieldIndex);
  return fieldOffset === 0 ? 0 : view.getInt32(root + fieldOffset, true);
}

function readStringField(
  data: Uint8Array,
  view: DataView,
  root: number,
  vtable: number,
  vtableSize: number,
  fieldIndex: number
): string {
  const fieldOffset = getFieldOffset(view, vtable, vtableSize, fieldIndex);
  if (fieldOffset === 0) {
    return '';
  }

  const relative = view.getUint32(root + fieldOffset, true);
  const stringStart = root + fieldOffset + relative;
  const stringLength = view.getUint32(stringStart, true);
  return decoder.decode(data.subarray(stringStart + 4, stringStart + 4 + stringLength));
}

function readDescriptorValue(state: FlatBufferState, descriptor: FlatBufferFieldDescriptor): unknown {
  if (descriptor.kind === 'int32') {
    return readInt32Field(state.view, state.root, state.vtable, state.vtableSize, descriptor.index);
  }

  return readStringField(state.data, state.view, state.root, state.vtable, state.vtableSize, descriptor.index);
}

function readDescriptorValueDirect(
  data: Uint8Array,
  view: DataView,
  root: number,
  vtable: number,
  vtableSize: number,
  descriptor: FlatBufferFieldDescriptor
): unknown {
  if (descriptor.kind === 'int32') {
    return readInt32Field(view, root, vtable, vtableSize, descriptor.index);
  }

  return readStringField(data, view, root, vtable, vtableSize, descriptor.index);
}

function createMappedExtractor(fieldDescriptors: Record<string, FlatBufferFieldDescriptor>): ArtifactRecordExtractor {
  return {
    getField(data: Uint8Array, fieldName: string): unknown {
      const descriptor = fieldDescriptors[fieldName];
      if (!descriptor) {
        return null;
      }
      return readDescriptorValue(createState(data), descriptor);
    },
    compileFieldAppender(fieldNames: string[]): (pendingArgs: unknown[], data: Uint8Array) => void {
      const descriptors = fieldNames.map((fieldName) => fieldDescriptors[fieldName] ?? null);

      switch (descriptors.length) {
        case 1: {
          const [descriptor0] = descriptors;
          return (pendingArgs: unknown[], data: Uint8Array): void => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null
            );
          };
        }
        case 2: {
          const [descriptor0, descriptor1] = descriptors;
          return (pendingArgs: unknown[], data: Uint8Array): void => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null
            );
          };
        }
        case 3: {
          const [descriptor0, descriptor1, descriptor2] = descriptors;
          return (pendingArgs: unknown[], data: Uint8Array): void => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null
            );
          };
        }
        case 4: {
          const [descriptor0, descriptor1, descriptor2, descriptor3] = descriptors;
          return (pendingArgs: unknown[], data: Uint8Array): void => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null,
              descriptor3 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor3) : null
            );
          };
        }
        default:
          return (pendingArgs: unknown[], data: Uint8Array): void => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            for (const descriptor of descriptors) {
              pendingArgs.push(
                descriptor ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor) : null
              );
            }
          };
      }
    },
    compileFieldValues(fieldNames: string[]): (data: Uint8Array) => unknown[] {
      const descriptors = fieldNames.map((fieldName) => fieldDescriptors[fieldName] ?? null);

      switch (descriptors.length) {
        case 1: {
          const [descriptor0] = descriptors;
          return (data: Uint8Array): unknown[] => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null
            ];
          };
        }
        case 2: {
          const [descriptor0, descriptor1] = descriptors;
          return (data: Uint8Array): unknown[] => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
            ];
          };
        }
        case 3: {
          const [descriptor0, descriptor1, descriptor2] = descriptors;
          return (data: Uint8Array): unknown[] => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null,
            ];
          };
        }
        case 4: {
          const [descriptor0, descriptor1, descriptor2, descriptor3] = descriptors;
          return (data: Uint8Array): unknown[] => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null,
              descriptor3 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor3) : null,
            ];
          };
        }
        default:
          return (data: Uint8Array): unknown[] => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return descriptors.map((descriptor) =>
              descriptor ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor) : null
            );
          };
      }
    },
    getFields(data: Uint8Array, fieldNames: string[]): Record<string, unknown> {
      const state = createState(data);
      return Object.fromEntries(
        fieldNames.map((fieldName) => {
          const descriptor = fieldDescriptors[fieldName];
          return [fieldName, descriptor ? readDescriptorValue(state, descriptor) : null];
        })
      );
    },
    getFieldValues(data: Uint8Array, fieldNames: string[]): unknown[] {
      const state = createState(data);
      return fieldNames.map((fieldName) => {
        const descriptor = fieldDescriptors[fieldName];
        return descriptor ? readDescriptorValue(state, descriptor) : null;
      });
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

export function extractArtifactFieldValues(
  extractor: ArtifactFieldExtractor,
  data: Uint8Array,
  fieldNames: string[]
): unknown[] {
  if (typeof extractor !== 'function' && extractor.getFieldValues) {
    return extractor.getFieldValues(data, fieldNames);
  }

  if (typeof extractor !== 'function' && extractor.getFields) {
    const fields = extractor.getFields(data, fieldNames);
    return fieldNames.map((fieldName) => fields[fieldName]);
  }

  return fieldNames.map((fieldName) => extractArtifactField(extractor, data, fieldName));
}

export function createArtifactFieldValueReader(
  extractor: ArtifactFieldExtractor,
  fieldNames: string[]
): (data: Uint8Array) => unknown[] {
  if (typeof extractor !== 'function' && extractor.compileFieldValues) {
    return extractor.compileFieldValues(fieldNames);
  }

  return (data: Uint8Array): unknown[] => extractArtifactFieldValues(extractor, data, fieldNames);
}

export function createArtifactFieldAppender(
  extractor: ArtifactFieldExtractor,
  fieldNames: string[]
): ((pendingArgs: unknown[], data: Uint8Array) => void) | null {
  if (typeof extractor !== 'function' && extractor.compileFieldAppender) {
    return extractor.compileFieldAppender(fieldNames);
  }

  return null;
}

export const demoExtractors: Record<string, ArtifactFieldExtractor> = {
  User: createMappedExtractor({
    id: { kind: 'int32', index: 0 },
    name: { kind: 'string', index: 1 },
    email: { kind: 'string', index: 2 },
    age: { kind: 'int32', index: 3 },
  }),
  Post: createMappedExtractor({
    id: { kind: 'int32', index: 0 },
    user_id: { kind: 'int32', index: 1 },
    title: { kind: 'string', index: 2 },
  }),
};
