# RememberIndexForRebuilding

## Location
src/backend/commands/tablecmds.c: 13760 - 13810

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
  - list_member_oid
  - get_index_constraint
  - RememberConstraintForRebuilding
  - pg_get_indexdef_string
  - lappend_oid
  - RememberReplicaIdentityForRebuilding
  - RememberClusterOnForRebuilding
  - AlteredTableInfo (struct)
- Called from (representative examples):
  - child_dependency_type
  - RememberAllDependentForRebuilding

## Notes and Other Information
- The deduplication check is critical for two reasons: preventing double recreation and ensuring definition strings are captured before any column type changes
- The function prioritizes constraint rebuilding over index rebuilding when an index belongs to a constraint
- Special index properties (replica identity, clustering) are preserved through dedicated helper functions
- Part of the broader ALTER TABLE infrastructure for handling type changes that require index rebuilds