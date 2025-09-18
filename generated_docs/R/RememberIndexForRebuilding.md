# RememberIndexForRebuilding

## Location
[src/backend/commands/tablecmds.c:13760-13810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13760-L13810)

## Overview
RememberIndexForRebuilding records an index that needs to be rebuilt during ALTER TABLE operations, with deduplication to ensure the same index isn't processed twice and proper handling of constraint-associated indexes.

## Definition
```c
static void RememberIndexForRebuilding(Oid indoid, AlteredTableInfo *tab)
```

## Detailed Description
This function is a critical subroutine for ATExecAlterColumnType that manages index rebuilding during column type alterations. It implements deduplication logic to prevent recreating the same index multiple times, which is essential when an index depends on multiple columns being altered. The function distinguishes between regular indexes and constraint-associated indexes, handling each appropriately. For constraint indexes, it delegates to RememberConstraintForRebuilding; for regular indexes, it captures the index definition and queues it for rebuilding while preserving special properties like replica identity and clustering.

## Parameters / Member Variables
- `indoid`: The OID of the index that needs to be remembered for rebuilding
- `tab`: Pointer to AlteredTableInfo structure that tracks all changes for the table being altered

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_oid](../l/list_member_oid.md)
  - [get_index_constraint](../g/get_index_constraint.md)
  - [RememberConstraintForRebuilding](RememberConstraintForRebuilding.md)
  - [pg_get_indexdef_string](../p/pg_get_indexdef_string.md)
  - lappend_oid
  - [RememberReplicaIdentityForRebuilding](RememberReplicaIdentityForRebuilding.md)
  - [RememberClusterOnForRebuilding](RememberClusterOnForRebuilding.md)
  - [AlteredTableInfo](../A/AlteredTableInfo.md) (struct)
- Called from (representative examples):
  - child_dependency_type
  - [RememberAllDependentForRebuilding](RememberAllDependentForRebuilding.md)

## Notes and Other Information
- The deduplication check is critical for two reasons: preventing double recreation and ensuring definition strings are captured before any column type changes
- The function prioritizes constraint rebuilding over index rebuilding when an index belongs to a constraint
- Special index properties (replica identity, clustering) are preserved through dedicated helper functions
- Part of the broader ALTER TABLE infrastructure for handling type changes that require index rebuilds