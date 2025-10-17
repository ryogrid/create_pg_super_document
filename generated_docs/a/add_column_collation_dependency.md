# add_column_collation_dependency

## Location
[src/backend/commands/tablecmds.c:7509-7531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7509-L7531)

## Overview
Establishes a dependency relationship between a table column and its collation to ensure referential integrity when collations are involved.

## Definition
```c
static void add_column_collation_dependency(Oid relid, int32 attnum, Oid collid)
```

## Detailed Description
This function creates a dependency entry that links a specific column of a table to its collation. The dependency ensures that a collation cannot be dropped while columns still depend on it for their sorting and comparison behavior. The function includes an optimization that skips recording dependencies for the default collation since it is pinned and cannot be dropped.

The function only records a dependency if the collation OID is valid and is not the default collation (DEFAULT_COLLATION_OID). This prevents unnecessary dependency records for the most common case while ensuring proper dependency tracking for custom collations.

## Parameters / Member Variables
- `relid`: The OID of the relation (table) containing the column
- `attnum`: The attribute number (column number) within the relation  
- `collid`: The OID of the collation used by the column

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (checks if collation OID is valid)
  - DEFAULT_COLLATION_OID (constant for default collation)
  - [recordDependencyOn](../r/recordDependencyOn.md) (creates the dependency record in pg_depend)
  - DEPENDENCY_NORMAL (dependency type constant)
  - CollationRelationId (system catalog relation ID for collations)
  - RelationRelationId (system catalog relation ID for relations)
- Called from (representative examples):
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (when adding new columns to tables)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md) (when changing column data types that may affect collation)

## Notes and Other Information
- The function is static, meaning it's only used within tablecmds.c
- Optimization: skips dependency recording for the default collation since it's pinned
- Uses DEPENDENCY_NORMAL type for the dependency relationship
- The objectSubId for collations is set to 0 since collations don't have sub-components
- Essential for maintaining data integrity when custom collations are used in column definitions
- Part of PostgreSQL's dependency tracking system for safe DDL operations

## Simplified Source

```c
static void
add_column_collation_dependency(Oid relid, int32 attnum, Oid collid)
{
    ObjectAddress myself, referenced;

    // Skip dependency recording for default collation (it's pinned)
    if (OidIsValid(collid) && collid != DEFAULT_COLLATION_OID) {
        // Set up the column as the dependent object
        myself.classId = RelationRelationId;
        myself.objectId = relid;
        myself.objectSubId = attnum;

        // Set up the collation as the referenced object
        referenced.classId = CollationRelationId;
        referenced.objectId = collid;
        referenced.objectSubId = 0;

        // Record the dependency in pg_depend
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }
}
```