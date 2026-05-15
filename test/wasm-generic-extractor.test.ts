import initFlatSQL from '../wasm/index.js';

const OMM_SCHEMA = `
  table OMM {
    CCSDS_OMM_VERS:double;
    CREATION_DATE:string;
    ORIGINATOR:string;
    OBJECT_NAME:string;
    OBJECT_ID:string;
    CENTER_NAME:string;
    REFERENCE_FRAME:RFM;
    REFERENCE_FRAME_EPOCH:string;
    TIME_SYSTEM:timingStandard = UTC;
    MEAN_ELEMENT_THEORY:meanElementSource = SGP4;
    COMMENT:string;
    EPOCH:string;
    SEMI_MAJOR_AXIS:double;
    MEAN_MOTION:double;
    ECCENTRICITY:double;
    INCLINATION:double;
    RA_OF_ASC_NODE:double;
    ARG_OF_PERICENTER:double;
    MEAN_ANOMALY:double;
    GM:double;
    MASS:double;
    SOLAR_RAD_AREA:double;
    SOLAR_RAD_COEFF:double;
    DRAG_AREA:double;
    DRAG_COEFF:double;
    EPHEMERIS_TYPE:ephemerisFormat = SGP4;
    CLASSIFICATION_TYPE:string;
    NORAD_CAT_ID:uint32;
    ELEMENT_SET_NO:uint32;
    REV_AT_EPOCH:double;
    BSTAR:double;
    MEAN_MOTION_DOT:double;
    MEAN_MOTION_DDOT:double;
    COV_REFERENCE_FRAME:RFM;
    COVARIANCE:[double];
    USER_DEFINED_BIP_0044_TYPE:uint;
    USER_DEFINED_OBJECT_DESIGNATOR:string;
    USER_DEFINED_EARTH_MODEL:string;
    USER_DEFINED_EPOCH_TIMESTAMP: double;
    USER_DEFINED_MICROSECONDS: double;
  }
  root_type OMM;
  file_identifier "$OMM";
`;

const STARLINK_6292_OMM = Buffer.from(
  'HAEAAEgAAAAkT01NAAAAADwAVAAAAAwACABQAEwAEAAAAAAAAAAAAAAARAAAADwANAAsACQAHAAUAAAAAAAAAAAAAAAAAAAABABIADwAAABQAAAAVAAAAGAAAAB4AAAAxEKtad4BV0DByqFFtsBwQGZmZmZmnGJAXf5D+u1/UUCej3xvHS04P22KKnBw9y1AUAAAAMfdAABkAAAAcAAAAAEAAABVAAAACAAAAFNETi1URVNUAAAAABQAAAAyMDI2LTA1LTExVDEwOjI2OjQxWgAAAAAFAAAARUFSVEgAAAAUAAAAMjAyNi0wNS0xMFQxMDo0NTozMVoAAAAACQAAADIwMjMtMDc4SgAAAA0AAABTVEFSTElOSy02MjkyAAAA',
  'base64',
).subarray(4);

describe('WASM generic FlatBuffer field extractor', () => {
  test('queries scalar and string fields without demo extractors', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(
      `
        table User {
          id: int (id);
          name: string;
          email: string (key);
          age: int;
        }
        root_type User;
      `,
      'generic-user',
    );

    db.registerFileId('USER', 'User');
    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    ]);

    expect(db.query('SELECT id, name, email, age FROM User WHERE age > 25')).toEqual({
      columns: ['id', 'name', 'email', 'age'],
      rows: [[1, 'Alice', 'alice@example.com', 30]],
    });

    db.destroy();
  });

  test('queries SDS OMM fields directly from raw FlatBuffers', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(OMM_SCHEMA, 'generic-omm');

    db.registerFileId('$OMM', 'OMM');
    db.ingestBuffers([STARLINK_6292_OMM]);

    expect(db.query('SELECT OBJECT_NAME, OBJECT_ID, NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID = 56775')).toEqual({
      columns: ['OBJECT_NAME', 'OBJECT_ID', 'NORAD_CAT_ID'],
      rows: [['STARLINK-6292', '2023-078J', 56775]],
    });

    db.destroy();
  });
});
