# add_column_collation_dependency

## Location
src/backend/commands/tablecmds.c: 7509 - 7531

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
  - recordDependencyOn (creates the dependency record in pg_depend)
  - DEPENDENCY_NORMAL (dependency type constant)
  - CollationRelationId (system catalog relation ID for collations)
  - RelationRelationId (system catalog relation ID for relations)
- Called from (representative examples):
  - ATExecAddColumn (when adding new columns to tables)
  - ATExecAlterColumnType (when changing column data types that may affect collation)

## Notes and Other Information
- The function is static, meaning it's only used within tablecmds.c
- Optimization: skips dependency recording for the default collation since it's pinned
- Uses DEPENDENCY_NORMAL type for the dependency relationship
- The objectSubId for collations is set to 0 since collations don't have sub-components
- Essential for maintaining data integrity when custom collations are used in column definitions
- Part of PostgreSQL's dependency tracking system for safe DDL operations