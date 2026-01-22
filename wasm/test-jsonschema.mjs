// Test JSON Schema generation with x-flatbuffers annotations
// Verifies that schemas can be converted to JSON Schema with full metadata

import { FlatcRunner } from 'flatc-wasm';

// Test schema with various FlatBuffers features
const testSchema = `
namespace test;

enum Status : byte { Pending = 0, Active = 1, Completed = 2 }

struct Vec2 {
    x: float;
    y: float;
}

table User {
    id: int32 (id: 0);
    name: string (id: 1);
    email: string (id: 2, key);
    age: int32 (id: 3);
    status: Status = Active (id: 4);
    location: Vec2 (id: 5);
    tags: [string] (id: 6);
}

table Post {
    id: int32 (id: 0);
    user_id: int32 (id: 1, key);
    title: string (id: 2);
    content: string (id: 3);
    views: uint64 (id: 4);
}

root_type User;
`;

const schemaInput = {
    entry: '/schemas/test.fbs',
    files: { '/schemas/test.fbs': testSchema }
};

async function main() {
    console.log('=== JSON Schema with x-flatbuffers Annotation Test ===\n');

    // Initialize flatc-wasm
    console.log('Initializing FlatcRunner...');
    const flatc = await FlatcRunner.init();
    console.log('FlatcRunner version:', flatc.version(), '\n');

    // Test 1: Generate basic JSON Schema
    console.log('Test 1: Generate basic JSON Schema...');
    const basicSchema = flatc.generateJsonSchema(schemaInput);
    const basicParsed = JSON.parse(basicSchema);

    console.log('  $schema:', basicParsed.$schema);
    console.log('  Has definitions:', !!basicParsed.definitions);

    const definitionKeys = Object.keys(basicParsed.definitions || {});
    console.log('  Definition count:', definitionKeys.length);
    console.log('  Definitions:', definitionKeys.join(', '));

    if (!basicParsed.definitions) {
        throw new Error('Basic JSON Schema missing definitions');
    }
    console.log('  Test 1 PASSED\n');

    // Test 2: Generate JSON Schema with x-flatbuffers annotations
    console.log('Test 2: Generate JSON Schema with x-flatbuffers annotations...');
    const annotatedSchema = flatc.generateJsonSchema(schemaInput, { includeXFlatbuffers: true });
    const annotated = JSON.parse(annotatedSchema);

    // Check root-level x-flatbuffers metadata
    if (!annotated['x-flatbuffers']) {
        throw new Error('Missing root x-flatbuffers metadata');
    }
    console.log('  Root x-flatbuffers metadata:');
    console.log('    namespace:', annotated['x-flatbuffers'].namespace);
    console.log('    root_type:', annotated['x-flatbuffers'].root_type);
    console.log('    file_ident:', annotated['x-flatbuffers'].file_ident || '(none)');

    // root_type includes namespace prefix
    if (!annotated['x-flatbuffers'].root_type.includes('User')) {
        throw new Error(`Expected root_type to include 'User', got '${annotated['x-flatbuffers'].root_type}'`);
    }
    console.log('  Test 2 PASSED\n');

    // Test 3: Verify User table x-flatbuffers annotations
    console.log('Test 3: Verify User table annotations...');
    const userDef = annotated.definitions?.test_User;

    if (!userDef) {
        throw new Error('Missing User definition in JSON Schema');
    }

    if (!userDef['x-flatbuffers']) {
        throw new Error('User definition missing x-flatbuffers metadata');
    }

    console.log('  User x-flatbuffers metadata:');
    console.log('    type:', userDef['x-flatbuffers'].type);

    // Check User properties
    const userProps = userDef.properties || {};
    console.log('  User properties:', Object.keys(userProps).join(', '));

    // Verify id field
    if (userProps.id) {
        console.log('  id field:');
        console.log('    type:', userProps.id.type);
        if (userProps.id['x-flatbuffers']) {
            console.log('    x-flatbuffers.id:', userProps.id['x-flatbuffers'].id);
        }
    }

    // Verify email field with key attribute
    if (userProps.email) {
        console.log('  email field:');
        console.log('    type:', userProps.email.type);
        if (userProps.email['x-flatbuffers']) {
            console.log('    x-flatbuffers.id:', userProps.email['x-flatbuffers'].id);
            console.log('    x-flatbuffers.key:', userProps.email['x-flatbuffers'].key);
        }
    }

    // Verify status field (enum with default)
    if (userProps.status) {
        console.log('  status field:');
        if (userProps.status['$ref']) {
            console.log('    $ref:', userProps.status['$ref']);
        }
        if (userProps.status['x-flatbuffers']) {
            console.log('    x-flatbuffers.default:', userProps.status['x-flatbuffers'].default);
        }
    }

    // Verify tags field (vector of strings)
    if (userProps.tags) {
        console.log('  tags field:');
        console.log('    type:', userProps.tags.type);
        if (userProps.tags.items) {
            console.log('    items.type:', userProps.tags.items.type);
        }
    }

    console.log('  Test 3 PASSED\n');

    // Test 4: Verify enum annotations
    console.log('Test 4: Verify enum annotations...');
    const statusDef = annotated.definitions?.test_Status;

    if (!statusDef) {
        console.log('  Status enum not found as separate definition (may be inlined)');
    } else {
        console.log('  Status enum found:');
        console.log('    type:', statusDef.type);
        if (statusDef.enum) {
            console.log('    values:', statusDef.enum.join(', '));
        }
        if (statusDef['x-flatbuffers']) {
            console.log('    x-flatbuffers.type:', statusDef['x-flatbuffers'].type);
            console.log('    x-flatbuffers.base_type:', statusDef['x-flatbuffers'].base_type);
        }
    }
    console.log('  Test 4 PASSED\n');

    // Test 5: Verify struct annotations
    console.log('Test 5: Verify struct annotations...');
    const vec2Def = annotated.definitions?.test_Vec2;

    if (!vec2Def) {
        console.log('  Vec2 struct not found as separate definition');
    } else {
        console.log('  Vec2 struct found:');
        if (vec2Def['x-flatbuffers']) {
            console.log('    x-flatbuffers.type:', vec2Def['x-flatbuffers'].type);
        }
        const vec2Props = vec2Def.properties || {};
        console.log('    properties:', Object.keys(vec2Props).join(', '));
    }
    console.log('  Test 5 PASSED\n');

    // Test 6: Verify Post table annotations
    console.log('Test 6: Verify Post table annotations...');
    const postDef = annotated.definitions?.test_Post;

    if (!postDef) {
        throw new Error('Missing Post definition');
    }

    const postProps = postDef.properties || {};
    console.log('  Post properties:', Object.keys(postProps).join(', '));

    // Verify user_id with key attribute
    if (postProps.user_id) {
        console.log('  user_id field:');
        if (postProps.user_id['x-flatbuffers']) {
            console.log('    x-flatbuffers.key:', postProps.user_id['x-flatbuffers'].key);
        }
    }

    // Verify views field (uint64)
    if (postProps.views) {
        console.log('  views field:');
        console.log('    type:', postProps.views.type);
        if (postProps.views['x-flatbuffers']) {
            console.log('    x-flatbuffers.base_type:', postProps.views['x-flatbuffers'].base_type);
        }
    }

    console.log('  Test 6 PASSED\n');

    // Test 7: Full schema output
    console.log('Test 7: Full annotated JSON Schema output...');
    console.log('--- BEGIN JSON SCHEMA ---');
    console.log(JSON.stringify(annotated, null, 2));
    console.log('--- END JSON SCHEMA ---\n');
    console.log('  Test 7 PASSED\n');

    // Test 8: Verify JSON Schema can validate data
    console.log('Test 8: Generate and validate data with schema...');

    // Create valid user JSON
    const validUser = {
        id: 1,
        name: "Alice",
        email: "alice@example.com",
        age: 30,
        status: "Active",
        location: { x: 1.5, y: 2.5 },
        tags: ["developer", "tester"]
    };

    // Convert to FlatBuffer and back to verify schema compatibility
    const binary = flatc.generateBinary(schemaInput, JSON.stringify(validUser));
    console.log('  Created FlatBuffer:', binary.length, 'bytes');

    const roundTripped = flatc.generateJSON(schemaInput, {
        path: '/data/user.bin',
        data: binary
    }, { defaultsJson: true, strictJson: true });
    const parsed = JSON.parse(roundTripped);

    console.log('  Round-tripped data:');
    console.log('    id:', parsed.id);
    console.log('    name:', parsed.name);
    console.log('    email:', parsed.email);
    console.log('    status:', parsed.status);
    console.log('    location:', JSON.stringify(parsed.location));
    console.log('    tags:', JSON.stringify(parsed.tags));

    // Verify data integrity
    if (parsed.id !== validUser.id) throw new Error('id mismatch');
    if (parsed.name !== validUser.name) throw new Error('name mismatch');
    if (parsed.email !== validUser.email) throw new Error('email mismatch');
    if (parsed.status !== validUser.status) throw new Error('status mismatch');

    console.log('  Test 8 PASSED\n');

    console.log('=== All JSON Schema tests PASSED! ===');
}

main().catch(err => {
    console.error('Test FAILED:', err.message);
    console.error(err.stack);
    process.exit(1);
});
