import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
const IDENTIFIER = '[A-Za-z_][A-Za-z0-9_]*';
const ROOT_TYPE_RE = new RegExp(`\\broot_type\\s+(${IDENTIFIER})\\s*;`);
const FILE_IDENTIFIER_RE = /\bfile_identifier\s+"([^"]+)"\s*;/;
const TABLE_RE = new RegExp(`\\btable\\s+(${IDENTIFIER})\\s*\\{`, 'g');
export function parseSchemaMetadata(name, source, filePath) {
    const rootType = ROOT_TYPE_RE.exec(source)?.[1] ?? name;
    const flatbufferIdentifier = FILE_IDENTIFIER_RE.exec(source)?.[1];
    const tableNames = [...source.matchAll(TABLE_RE)].map((match) => match[1]);
    return {
        name,
        path: filePath,
        rootType,
        flatbufferIdentifier,
        tableNames,
    };
}
export async function discoverSdsSchemas(schemaRoot) {
    const entries = await readdir(schemaRoot, { withFileTypes: true });
    const schemaDirs = entries
        .filter((entry) => entry.isDirectory())
        .map((entry) => entry.name)
        .sort((left, right) => left.localeCompare(right));
    const schemas = [];
    for (const name of schemaDirs) {
        const filePath = join(schemaRoot, name, 'main.fbs');
        try {
            const source = await readFile(filePath, 'utf8');
            schemas.push(parseSchemaMetadata(name, source, filePath));
        }
        catch (error) {
            if (error.code !== 'ENOENT') {
                throw error;
            }
        }
    }
    return schemas;
}
//# sourceMappingURL=sds-discovery.js.map