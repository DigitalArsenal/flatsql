# Junction Tables Architecture for FlatSQL

## Overview

FlatSQL needs to handle all relationship types that FlatBuffers supports, with proper
storage in separate tables for imported types, junction tables for relationships,
and cascading operations to maintain referential integrity.

## FlatBuffers Relationship Types

### 1. Single Table Reference
```fbs
table Monster {
    weapon: Weapon;  // 0..1 relationship
}
```
- Junction: `Monster__weapon` (parent_rowid → child_rowid)
- Cascade: DELETE Monster → DELETE junction row → optionally DELETE Weapon if orphaned

### 2. Vector of Tables
```fbs
table Monster {
    weapons: [Weapon];  // 0..N relationship
}
```
- Junction: `Monster__weapons` (parent_rowid, index, child_rowid)
- Index preserves array ordering
- Cascade: DELETE Monster → DELETE all junction rows → cleanup orphaned Weapons

### 3. Union Type
```fbs
union Equipment { Weapon, Armor, Shield }
table Monster {
    equipment: Equipment;  // 0..1, polymorphic
}
```
- Junction: `Monster__equipment` (parent_rowid, union_type, child_rowid)
- `union_type` is the discriminator ("Weapon", "Armor", "Shield")
- Routes to correct child table based on type
- Cascade: DELETE Monster → DELETE junction → cleanup typed table

### 4. Vector of Unions
```fbs
table Monster {
    items: [Equipment];  // 0..N, polymorphic
}
```
- Junction: `Monster__items` (parent_rowid, index, union_type, child_rowid)
- Combines vector indexing with union type discrimination
- Cascade: Same as above but for all entries

### 5. Nested Structs (NO junction needed)
```fbs
struct Vec3 { x: float; y: float; z: float; }
table Monster {
    pos: Vec3;  // Inline in FlatBuffer, no separate storage
}
```
- Structs are stored inline in the parent FlatBuffer
- No junction table needed
- Extracted via field extractors for querying

## Import Handling

### Schema Registry
```
SchemaRegistry {
    schemas: Map<filename, ParsedSchema>
    imports: Map<filename, Set<filename>>  // dependency graph
    tables: Map<table_name, TableInfo>
    unions: Map<union_name, UnionInfo>
}
```

### Circular Dependency Detection
```
Algorithm: DFS with cycle detection

1. Build import graph from all .fbs files
2. For each schema, DFS traverse imports
3. Track visited set and recursion stack
4. If node in recursion stack → CYCLE DETECTED
5. Report cycle path: A → B → C → A
```

### Resolution Order
```
Topological sort of import graph:
1. Leaf schemas first (no imports)
2. Then schemas that only import leaves
3. Continue until root schemas
4. Cycles prevent resolution → error
```

## Junction Table Schema

### Standard Junction Table
```sql
CREATE TABLE {parent}__{field} (
    id INTEGER PRIMARY KEY,
    parent_rowid INTEGER NOT NULL,
    child_rowid INTEGER NOT NULL,

    -- For vectors:
    vec_index INTEGER,  -- NULL for single refs

    -- For unions:
    union_type TEXT,    -- NULL for non-unions

    -- Metadata:
    created_at INTEGER DEFAULT (strftime('%s', 'now')),

    FOREIGN KEY (parent_rowid) REFERENCES {parent}(_rowid) ON DELETE CASCADE,
    FOREIGN KEY (child_rowid) REFERENCES {child}(_rowid) ON DELETE CASCADE
);

CREATE INDEX idx_{parent}__{field}_parent ON {parent}__{field}(parent_rowid);
CREATE INDEX idx_{parent}__{field}_child ON {parent}__{field}(child_rowid);
```

### Naming Convention
- Junction table: `{ParentTable}__{field_name}`
- Double underscore separates parent from field
- Examples:
  - `Monster__weapon` (single)
  - `Monster__weapons` (vector)
  - `Monster__equipment` (union)

## Cascade Operations

### DELETE Parent
```
1. Trigger fires on parent DELETE
2. Find all junction rows where parent_rowid = deleted_id
3. For each junction row:
   a. Record child_rowid and child_table
   b. DELETE junction row
4. For each unique (child_table, child_rowid):
   a. Check if any other junction references this child
   b. If orphaned (ref_count = 0):
      - DELETE from child table
      - Recursively cascade if child has its own junctions
```

### Reference Counting (Optional)
```
Child tables can have ref_count column:
- INCREMENT on junction INSERT
- DECREMENT on junction DELETE
- DELETE child when ref_count reaches 0

Pros: O(1) orphan check
Cons: Extra bookkeeping, potential for count drift
```

### Deferred Cleanup (Alternative)
```
Instead of immediate cascade:
1. Mark junction rows as deleted (soft delete)
2. Background process periodically:
   a. Find children with no active junction refs
   b. DELETE orphaned children
   c. Purge soft-deleted junction rows

Pros: Better write performance
Cons: Storage overhead, eventual consistency
```

## Implementation Plan

### Phase 1: Schema Analysis
- [ ] Parse FlatBuffers schema for table references
- [ ] Identify imported types vs inline structs
- [ ] Build import dependency graph
- [ ] Implement cycle detection algorithm
- [ ] Determine which types need separate tables

### Phase 2: Junction Table Generation
- [ ] Auto-generate junction table DDL from schema
- [ ] Create junction tables on database init
- [ ] Generate indexes for efficient lookups

### Phase 3: Insert Operations
- [ ] When inserting parent with nested tables:
  - [ ] Extract nested FlatBuffers
  - [ ] Insert into child tables
  - [ ] Create junction table entries
  - [ ] Maintain vector ordering

### Phase 4: Query Operations
- [ ] JOIN queries across junction tables
- [ ] Reconstruct full objects from junctions
- [ ] Support filtering on child properties

### Phase 5: Cascade Operations
- [ ] DELETE triggers for cascade
- [ ] UPDATE handling for rowid changes
- [ ] Orphan cleanup strategy

### Phase 6: Testing
- [ ] Unit tests for each relationship type
- [ ] Cycle detection tests
- [ ] Cascade operation tests
- [ ] Performance tests with deep nesting

## Example Schema

```fbs
// weapons.fbs
namespace game;
table Weapon {
    name: string;
    damage: int;
    enchantments: [Enchantment];
}
table Enchantment {
    name: string;
    power: int;
}

// armor.fbs
namespace game;
table Armor {
    name: string;
    defense: int;
}

// monster.fbs
include "weapons.fbs";
include "armor.fbs";

namespace game;

union Equipment { Weapon, Armor }

table Monster {
    name: string (key);
    hp: int;
    primary_weapon: Weapon;      // 1:1 junction
    inventory: [Weapon];          // 1:N junction with index
    equipped: Equipment;          // 1:1 union junction
    loot_table: [Equipment];      // 1:N union junction
}

root_type Monster;
```

### Generated Tables
```
Tables:
  - Monster (main table)
  - Weapon (imported type table)
  - Armor (imported type table)
  - Enchantment (imported type table)

Junctions:
  - Monster__primary_weapon (parent_rowid, child_rowid)
  - Monster__inventory (parent_rowid, vec_index, child_rowid)
  - Monster__equipped (parent_rowid, union_type, child_rowid)
  - Monster__loot_table (parent_rowid, vec_index, union_type, child_rowid)
  - Weapon__enchantments (parent_rowid, vec_index, child_rowid)

Cascade Chain:
  DELETE Monster
    → DELETE Monster__* junction rows
    → If Weapon orphaned: DELETE Weapon
      → DELETE Weapon__enchantments junction rows
      → If Enchantment orphaned: DELETE Enchantment
```

## API Design

```cpp
class JunctionManager {
public:
    // Schema analysis
    void analyzeSchema(const std::string& fbs_content);
    bool hasCircularDependency() const;
    std::vector<std::string> getImportOrder() const;

    // Table management
    void createJunctionTables();
    std::vector<std::string> getJunctionTablesFor(const std::string& table);

    // Insert with junction handling
    uint64_t insertWithRelations(
        const std::string& table,
        const std::vector<uint8_t>& fb_data
    );

    // Query with joins
    QueryResult queryWithJoins(
        const std::string& sql,
        bool includeChildren = true
    );

    // Cascade operations
    void deleteWithCascade(const std::string& table, uint64_t rowid);
    void cleanupOrphans(const std::string& table);
};
```

## Open Questions

1. **Shared instances**: Can two Monsters reference the same Weapon instance?
   - If yes: Need reference counting
   - If no: Each insert creates new child rows (simpler)

2. **Lazy vs eager loading**: When querying Monster, always load children?
   - Option A: Always JOIN and reconstruct full graph
   - Option B: Return parent only, load children on demand
   - Option C: Configurable per-query

3. **Update semantics**: What happens on UPDATE Monster with new weapon?
   - Option A: Delete old junction, create new (with cascade)
   - Option B: Update junction to point to new child
   - Option C: In-place update of child FlatBuffer

4. **Cross-schema references**: Monster in schema A references Weapon in schema B?
   - Need unified namespace management
   - Schema B must be loaded before Schema A
