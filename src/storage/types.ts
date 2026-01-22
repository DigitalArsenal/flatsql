// Storage types for stacked FlatBuffers

export interface RecordHeader {
  sequence: bigint;
  tableName: string;
  timestamp: bigint;
  dataLength: number;
  checksum: number;
}

export interface DataFileHeader {
  magic: number; // 0x464C5451 = "FLTQ"
  version: number;
  dataStartOffset: bigint;
  recordCount: bigint;
  schemaName: string;
}

export interface StoredRecord {
  header: RecordHeader;
  offset: bigint;
  data: Uint8Array;
}

export const MAGIC = 0x464c5451; // "FLTQ"
export const VERSION = 1;

// Simple CRC32 implementation
export function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  const table = getCRC32Table();

  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ table[(crc ^ data[i]) & 0xff];
  }

  return (crc ^ 0xffffffff) >>> 0;
}

let crc32Table: Uint32Array | null = null;

function getCRC32Table(): Uint32Array {
  if (crc32Table) return crc32Table;

  crc32Table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    crc32Table[i] = c;
  }
  return crc32Table;
}
