const EMAIL_PROVIDERS = [
    'gmail.com',
    'yahoo.com',
    'outlook.com',
    'hotmail.com',
    'icloud.com',
    'protonmail.com',
    'aol.com',
    'zoho.com',
    'fastmail.com',
    'tutanota.com',
    'yandex.com',
    'mail.com',
    'gmx.com',
    'hey.com',
    'pm.me'
];

const TELEMETRY_SUBSYSTEMS = ['POWER', 'ADCS', 'THERMAL', 'COMMS', 'PAYLOAD', 'PROP'];
const TELEMETRY_MODES = ['NOMINAL', 'SAFE', 'SCIENCE', 'CALIBRATION'];

export const DEMO_DATASET_ORDER = ['user', 'mpe', 'telemetry'];

export const DEMO_DATASETS = {
    user: {
        key: 'user',
        optionLabel: 'User Directory',
        standardLabel: 'Application Profile Demo',
        description: 'Synthetic application users for fast key and text-filter queries.',
        tableName: 'User',
        fileId: 'USER',
        recordLabel: 'User',
        accentColor: '#4ade80',
        schema: `
            table User {
                id: int (id);
                name: string;
                email: string (key);
                age: int;
            }
            root_type User;
        `,
        jsonSchema: {
            $schema: 'https://json-schema.org/draft/2020-12/schema',
            'x-flatbuffers': {
                namespace: 'demo',
                root_type: 'demo_User',
                file_ident: 'USER'
            },
            definitions: {
                demo_User: {
                    type: 'object',
                    'x-flatbuffers': { type: 'table' },
                    properties: {
                        id: { type: 'integer', 'x-flatbuffers': { id: 0, base_type: 'int32' } },
                        name: { type: 'string', 'x-flatbuffers': { id: 1 } },
                        email: { type: 'string', 'x-flatbuffers': { id: 2, key: true } },
                        age: { type: 'integer', 'x-flatbuffers': { id: 3, base_type: 'int32' } }
                    }
                }
            }
        },
        defaultQuery: 'SELECT id, name, email, age FROM User LIMIT 50',
        queryPlaceholder: "SELECT * FROM User WHERE email LIKE '%gmail%' LIMIT 50",
        exampleQueries: [
            { label: 'All Users', sql: 'SELECT id, name, email, age FROM User LIMIT 50' },
            { label: 'Gmail', sql: "SELECT id, name, email FROM User WHERE email LIKE '%gmail%' LIMIT 50" },
            { label: 'Age > 40', sql: 'SELECT id, name, age FROM User WHERE age > 40 LIMIT 50' },
            { label: 'Count', sql: 'SELECT COUNT(*) as total FROM User' }
        ]
    },
    mpe: {
        key: 'mpe',
        optionLabel: 'CCSDS OMM / MPE',
        standardLabel: 'CCSDS Orbit Mean-Elements Message',
        description: 'Minimum Propagatable Element records derived from the CCSDS OMM standard.',
        tableName: 'MPE',
        fileId: '$MPE',
        recordLabel: 'MPE',
        accentColor: '#60a5fa',
        schema: `
            table MPE {
                ENTITY_ID: string (key);
                EPOCH: double;
                MEAN_MOTION: double;
                ECCENTRICITY: double;
                INCLINATION: double;
                RA_OF_ASC_NODE: double;
                ARG_OF_PERICENTER: double;
                MEAN_ANOMALY: double;
                BSTAR: double;
                MEAN_ELEMENT_THEORY: int;
            }
            root_type MPE;
        `,
        jsonSchema: {
            $schema: 'https://json-schema.org/draft/2020-12/schema',
            'x-flatbuffers': {
                namespace: 'demo',
                root_type: 'demo_MPE',
                file_ident: '$MPE'
            },
            definitions: {
                demo_MPE: {
                    type: 'object',
                    'x-flatbuffers': { type: 'table' },
                    properties: {
                        ENTITY_ID: { type: 'string', 'x-flatbuffers': { id: 0, key: true } },
                        EPOCH: { type: 'number', 'x-flatbuffers': { id: 1, base_type: 'float64' } },
                        MEAN_MOTION: { type: 'number', 'x-flatbuffers': { id: 2, base_type: 'float64' } },
                        ECCENTRICITY: { type: 'number', 'x-flatbuffers': { id: 3, base_type: 'float64' } },
                        INCLINATION: { type: 'number', 'x-flatbuffers': { id: 4, base_type: 'float64' } },
                        RA_OF_ASC_NODE: { type: 'number', 'x-flatbuffers': { id: 5, base_type: 'float64' } },
                        ARG_OF_PERICENTER: { type: 'number', 'x-flatbuffers': { id: 6, base_type: 'float64' } },
                        MEAN_ANOMALY: { type: 'number', 'x-flatbuffers': { id: 7, base_type: 'float64' } },
                        BSTAR: { type: 'number', 'x-flatbuffers': { id: 8, base_type: 'float64' } },
                        MEAN_ELEMENT_THEORY: { type: 'integer', 'x-flatbuffers': { id: 9, base_type: 'int32' } }
                    }
                }
            }
        },
        defaultQuery: 'SELECT ENTITY_ID, EPOCH, INCLINATION, MEAN_MOTION FROM MPE LIMIT 50',
        queryPlaceholder: 'SELECT ENTITY_ID, BSTAR FROM MPE WHERE BSTAR > 0.00015 LIMIT 50',
        exampleQueries: [
            { label: 'Catalog', sql: 'SELECT ENTITY_ID, EPOCH, INCLINATION, MEAN_MOTION FROM MPE LIMIT 50' },
            { label: 'Sun-Sync', sql: 'SELECT ENTITY_ID, INCLINATION, MEAN_ELEMENT_THEORY FROM MPE WHERE INCLINATION BETWEEN 96 AND 99 LIMIT 50' },
            { label: 'High Drag', sql: 'SELECT ENTITY_ID, BSTAR, ECCENTRICITY FROM MPE WHERE BSTAR > 0.00015 LIMIT 50' },
            { label: 'Count', sql: 'SELECT COUNT(*) as total FROM MPE' }
        ]
    },
    telemetry: {
        key: 'telemetry',
        optionLabel: 'CCSDS Telemetry Packet',
        standardLabel: 'CCSDS Spacecraft Telemetry',
        description: 'Compact telemetry packets with subsystem, mode, thermal, and link-budget fields.',
        tableName: 'Telemetry',
        fileId: 'TELE',
        recordLabel: 'Telemetry',
        accentColor: '#f59e0b',
        schema: `
            table Telemetry {
                packet_id: int (id);
                spacecraft: string;
                subsystem: string;
                mode: string;
                temperature_c: int;
                signal_db: int;
                timestamp_s: int;
            }
            root_type Telemetry;
        `,
        jsonSchema: {
            $schema: 'https://json-schema.org/draft/2020-12/schema',
            'x-flatbuffers': {
                namespace: 'demo',
                root_type: 'demo_Telemetry',
                file_ident: 'TELE'
            },
            definitions: {
                demo_Telemetry: {
                    type: 'object',
                    'x-flatbuffers': { type: 'table' },
                    properties: {
                        packet_id: { type: 'integer', 'x-flatbuffers': { id: 0, base_type: 'int32' } },
                        spacecraft: { type: 'string', 'x-flatbuffers': { id: 1 } },
                        subsystem: { type: 'string', 'x-flatbuffers': { id: 2 } },
                        mode: { type: 'string', 'x-flatbuffers': { id: 3 } },
                        temperature_c: { type: 'integer', 'x-flatbuffers': { id: 4, base_type: 'int32' } },
                        signal_db: { type: 'integer', 'x-flatbuffers': { id: 5, base_type: 'int32' } },
                        timestamp_s: { type: 'integer', 'x-flatbuffers': { id: 6, base_type: 'int32' } }
                    }
                }
            }
        },
        defaultQuery: 'SELECT packet_id, spacecraft, subsystem, temperature_c, mode FROM Telemetry LIMIT 50',
        queryPlaceholder: "SELECT packet_id, spacecraft, signal_db FROM Telemetry WHERE subsystem = 'POWER' LIMIT 50",
        exampleQueries: [
            { label: 'Packets', sql: 'SELECT packet_id, spacecraft, subsystem, temperature_c, mode FROM Telemetry LIMIT 50' },
            { label: 'Low Signal', sql: 'SELECT packet_id, spacecraft, signal_db FROM Telemetry WHERE signal_db < 45 LIMIT 50' },
            { label: 'Subsystem Mix', sql: 'SELECT subsystem, COUNT(*) as packets FROM Telemetry GROUP BY subsystem ORDER BY packets DESC' },
            { label: 'Latest', sql: 'SELECT packet_id, spacecraft, timestamp_s FROM Telemetry ORDER BY timestamp_s DESC LIMIT 50' }
        ]
    }
};

export function listDemoDatasets() {
    return DEMO_DATASET_ORDER.map((key) => DEMO_DATASETS[key]);
}

export function getDemoDataset(datasetKey) {
    return DEMO_DATASETS[datasetKey] || DEMO_DATASETS.user;
}

function randomProvider(id) {
    return EMAIL_PROVIDERS[id % EMAIL_PROVIDERS.length];
}

function createUserRecord(flatsql, id) {
    const age = 20 + (id % 50);
    const name = `User${id}`;
    const email = `user${id}@${randomProvider(id)}`;
    return {
        buffer: flatsql.createTestUser(id, name, email, age),
        preview: { id, name, email, age }
    };
}

function createMPERecord(flatsql, id) {
    const entityId = String(60000 + id);
    const epoch = 1710000000 + (id * 900);
    const meanMotion = 11.8 + ((id % 40) * 0.09);
    const eccentricity = Number(((id % 200) / 100000).toFixed(6));
    const inclination = 48 + ((id * 7) % 5200) / 100;
    const raOfAscNode = (id * 13) % 360;
    const argOfPericenter = (id * 17) % 360;
    const meanAnomaly = (id * 19) % 360;
    const bstar = Number((0.00002 + ((id % 12) * 0.00002)).toFixed(6));
    const meanElementTheory = id % 3;

    return {
        buffer: flatsql.createTestMPE(
            entityId,
            epoch,
            meanMotion,
            eccentricity,
            inclination,
            raOfAscNode,
            argOfPericenter,
            meanAnomaly,
            bstar,
            meanElementTheory
        ),
        preview: {
            ENTITY_ID: entityId,
            INCLINATION: inclination.toFixed(2),
            BSTAR: bstar.toFixed(6),
            MEAN_ELEMENT_THEORY: meanElementTheory
        }
    };
}

function createTelemetryRecord(flatsql, id) {
    const packetId = id;
    const spacecraft = `SAT-${String((id % 8) + 1).padStart(2, '0')}`;
    const subsystem = TELEMETRY_SUBSYSTEMS[id % TELEMETRY_SUBSYSTEMS.length];
    const mode = TELEMETRY_MODES[id % TELEMETRY_MODES.length];
    const temperatureC = 18 + ((id * 3) % 62);
    const signalDb = 32 + ((id * 5) % 28);
    const timestampS = 1710000000 + (id * 15);

    return {
        buffer: flatsql.createTestTelemetry(
            packetId,
            spacecraft,
            subsystem,
            mode,
            temperatureC,
            signalDb,
            timestampS
        ),
        preview: {
            packet_id: packetId,
            spacecraft,
            subsystem,
            mode
        }
    };
}

export function createDemoRecords(flatsql, datasetKey, count, startId = 0) {
    const dataset = getDemoDataset(datasetKey);
    const buffers = [];
    const samples = [];

    for (let i = 0; i < count; i++) {
        const ordinal = startId + i + 1;
        let record;

        switch (dataset.key) {
            case 'mpe':
                record = createMPERecord(flatsql, ordinal);
                break;
            case 'telemetry':
                record = createTelemetryRecord(flatsql, ordinal);
                break;
            case 'user':
            default:
                record = createUserRecord(flatsql, ordinal);
                break;
        }

        buffers.push(record.buffer);
        if (samples.length < 5) {
            samples.push({
                fb: Array.from(record.buffer),
                preview: record.preview
            });
        }
    }

    return { dataset, buffers, samples };
}
