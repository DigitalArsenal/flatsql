// Stacked FlatBuffers storage implementation
// Files are append-only sequences of FlatBuffer records with headers
// Fully readable by standard FlatBuffer tools
import { MAGIC, VERSION, crc32 } from './types.js';
// Header sizes (fixed layout for easy seeking)
const FILE_HEADER_SIZE = 64; // Padded for alignment
const RECORD_HEADER_SIZE = 48; // Fixed size record header
// Security: Default maximum storage size (1GB)
const DEFAULT_MAX_STORAGE_SIZE = 1024 * 1024 * 1024;
// Security: Warning threshold (80% of max)
const WARNING_THRESHOLD = 0.8;
export class StackedFlatBufferStore {
    data;
    dataView;
    writeOffset;
    recordCount = 0n;
    sequence = 0n;
    schemaName;
    // Index of records by offset for quick lookup
    recordIndex = new Map();
    // Security: Storage limits
    maxSize;
    warningEmitted = false;
    onStorageWarning;
    constructor(schemaName, options = {}) {
        // Support legacy signature: constructor(schemaName, initialCapacity)
        const opts = typeof options === 'number'
            ? { initialCapacity: options }
            : options;
        this.schemaName = schemaName;
        this.maxSize = opts.maxSize ?? DEFAULT_MAX_STORAGE_SIZE;
        this.onStorageWarning = opts.onStorageWarning;
        const initialCapacity = opts.initialCapacity ?? 1024 * 1024;
        this.data = new Uint8Array(initialCapacity);
        this.dataView = new DataView(this.data.buffer);
        this.writeOffset = BigInt(FILE_HEADER_SIZE);
        this.writeFileHeader();
    }
    writeFileHeader() {
        const view = this.dataView;
        // Magic number
        view.setUint32(0, MAGIC, true);
        // Version
        view.setUint32(4, VERSION, true);
        // Data start offset
        view.setBigUint64(8, BigInt(FILE_HEADER_SIZE), true);
        // Record count (updated on each append)
        view.setBigUint64(16, 0n, true);
        // Schema name (up to 40 bytes, null-terminated)
        const nameBytes = new TextEncoder().encode(this.schemaName);
        const nameLen = Math.min(nameBytes.length, 39);
        this.data.set(nameBytes.slice(0, nameLen), 24);
        this.data[24 + nameLen] = 0;
    }
    updateFileHeader() {
        this.dataView.setBigUint64(16, this.recordCount, true);
    }
    ensureCapacity(needed) {
        const totalNeeded = Number(this.writeOffset) + needed;
        if (totalNeeded <= this.data.length)
            return;
        // Security: Check if we would exceed max size
        if (this.maxSize > 0 && totalNeeded > this.maxSize) {
            throw new Error(`Storage size limit exceeded: requested ${totalNeeded} bytes, ` +
                `maximum is ${this.maxSize} bytes`);
        }
        // Grow by doubling
        let newSize = this.data.length * 2;
        while (newSize < totalNeeded) {
            newSize *= 2;
        }
        // Security: Cap at max size
        if (this.maxSize > 0 && newSize > this.maxSize) {
            newSize = this.maxSize;
        }
        // Security: Emit warning when approaching limit
        if (this.maxSize > 0 && !this.warningEmitted) {
            const threshold = this.maxSize * WARNING_THRESHOLD;
            if (newSize >= threshold) {
                this.warningEmitted = true;
                if (this.onStorageWarning) {
                    this.onStorageWarning(newSize, this.maxSize);
                }
            }
        }
        const newData = new Uint8Array(newSize);
        newData.set(this.data);
        this.data = newData;
        this.dataView = new DataView(this.data.buffer);
    }
    // Append a FlatBuffer record
    // Returns the offset where the record was stored
    append(tableName, flatbufferData) {
        const checksum = crc32(flatbufferData);
        const timestamp = BigInt(Date.now());
        const seq = this.sequence++;
        const header = {
            sequence: seq,
            tableName,
            timestamp,
            dataLength: flatbufferData.length,
            checksum,
        };
        const totalSize = RECORD_HEADER_SIZE + flatbufferData.length;
        this.ensureCapacity(totalSize);
        const recordOffset = this.writeOffset;
        // Write record header
        const offset = Number(this.writeOffset);
        this.dataView.setBigUint64(offset, seq, true);
        // Table name (16 bytes, null-padded)
        const tableBytes = new TextEncoder().encode(tableName);
        const tableLen = Math.min(tableBytes.length, 15);
        this.data.set(tableBytes.slice(0, tableLen), offset + 8);
        this.data[offset + 8 + tableLen] = 0;
        this.dataView.setBigUint64(offset + 24, timestamp, true);
        this.dataView.setUint32(offset + 32, flatbufferData.length, true);
        this.dataView.setUint32(offset + 36, checksum, true);
        // Reserved bytes 40-47
        // Write FlatBuffer data
        this.data.set(flatbufferData, offset + RECORD_HEADER_SIZE);
        this.writeOffset += BigInt(totalSize);
        this.recordCount++;
        this.recordIndex.set(recordOffset, header);
        this.updateFileHeader();
        return recordOffset;
    }
    // Read a record at a given offset
    readRecord(offset) {
        const off = Number(offset);
        // Read header
        const sequence = this.dataView.getBigUint64(off, true);
        const tableNameBytes = this.data.slice(off + 8, off + 24);
        const nullIdx = tableNameBytes.indexOf(0);
        const tableName = new TextDecoder().decode(tableNameBytes.slice(0, nullIdx >= 0 ? nullIdx : tableNameBytes.length));
        const timestamp = this.dataView.getBigUint64(off + 24, true);
        const dataLength = this.dataView.getUint32(off + 32, true);
        const checksum = this.dataView.getUint32(off + 36, true);
        // Read data
        const dataStart = off + RECORD_HEADER_SIZE;
        const data = this.data.slice(dataStart, dataStart + dataLength);
        // Verify checksum
        const computedChecksum = crc32(data);
        if (computedChecksum !== checksum) {
            throw new Error(`Checksum mismatch at offset ${offset}: expected ${checksum}, got ${computedChecksum}`);
        }
        return {
            header: {
                sequence,
                tableName,
                timestamp,
                dataLength,
                checksum,
            },
            offset,
            data,
        };
    }
    // Iterate all records
    *iterateRecords() {
        let offset = BigInt(FILE_HEADER_SIZE);
        const end = this.writeOffset;
        while (offset < end) {
            const record = this.readRecord(offset);
            yield record;
            offset += BigInt(RECORD_HEADER_SIZE + record.header.dataLength);
        }
    }
    // Get records for a specific table
    *iterateTableRecords(tableName) {
        for (const record of this.iterateRecords()) {
            if (record.header.tableName === tableName) {
                yield record;
            }
        }
    }
    // Get the raw data (for export/persistence)
    getData() {
        return this.data.slice(0, Number(this.writeOffset));
    }
    // Load from existing data
    static fromData(data, options = {}) {
        const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
        // Verify magic
        const magic = view.getUint32(0, true);
        if (magic !== MAGIC) {
            throw new Error(`Invalid file magic: expected ${MAGIC.toString(16)}, got ${magic.toString(16)}`);
        }
        const version = view.getUint32(4, true);
        if (version !== VERSION) {
            throw new Error(`Unsupported version: ${version}`);
        }
        // Read schema name
        const nameBytes = data.slice(24, 64);
        const nullIdx = nameBytes.indexOf(0);
        const schemaName = new TextDecoder().decode(nameBytes.slice(0, nullIdx >= 0 ? nullIdx : nameBytes.length));
        const store = new StackedFlatBufferStore(schemaName, {
            ...options,
            initialCapacity: data.length,
        });
        store.data.set(data);
        store.dataView = new DataView(store.data.buffer);
        // Rebuild index by scanning records
        store.writeOffset = BigInt(FILE_HEADER_SIZE);
        store.recordCount = 0n;
        store.sequence = 0n;
        // Find end and rebuild index
        let offset = BigInt(FILE_HEADER_SIZE);
        while (offset < BigInt(data.length)) {
            try {
                const record = store.readRecord(offset);
                store.recordIndex.set(offset, record.header);
                store.recordCount++;
                if (record.header.sequence >= store.sequence) {
                    store.sequence = record.header.sequence + 1n;
                }
                offset += BigInt(RECORD_HEADER_SIZE + record.header.dataLength);
                store.writeOffset = offset;
            }
            catch {
                // End of valid data
                break;
            }
        }
        return store;
    }
    getRecordCount() {
        return this.recordCount;
    }
    getSchemaName() {
        return this.schemaName;
    }
    // Security: Get current storage size
    getCurrentSize() {
        return Number(this.writeOffset);
    }
    // Security: Get maximum storage size (0 = unlimited)
    getMaxSize() {
        return this.maxSize;
    }
    // Security: Get storage usage as a percentage (0-100)
    getUsagePercent() {
        if (this.maxSize === 0)
            return 0;
        return (Number(this.writeOffset) / this.maxSize) * 100;
    }
    // Security: Check if storage is near capacity (>80%)
    isNearCapacity() {
        if (this.maxSize === 0)
            return false;
        return Number(this.writeOffset) >= this.maxSize * WARNING_THRESHOLD;
    }
}
//# sourceMappingURL=stacked-flatbuffers.js.map