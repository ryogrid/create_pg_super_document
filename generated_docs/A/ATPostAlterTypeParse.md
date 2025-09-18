# ATPostAlterTypeParse

## Location
src/backend/commands/tablecmds.c: 14031 - 14246

## Overview
ATPostAlterTypeParse re-parses previously saved definition strings for constraints, indexes, or statistics objects against new column data types and queues the resulting commands for execution.

## Definition
```c
static void ATPostAlterTypeParse(Oid oldId, Oid oldRelId, Oid refRelId, char *cmd, List **wqueue, LOCKMODE lockmode, bool rewrite)
```

## Detailed Description
This function handles the critical task of re-creating database objects (indexes, constraints, statistics) after column type changes. It parses the previously captured definition strings using the raw parser, then transforms them through appropriate parse utilities for different statement types. The function handles IndexStmt, AlterTableStmt, CreateStatsStmt, and AlterDomainStmt, converting them into work queue entries with modified subtypes for re-addition. It preserves object comments and handles special cases like foreign key constraint reuse and tablespace settings. The function also coordinates with RebuildConstraintComment to ensure constraint comments are properly restored.

## Parameters / Member Variables
- `oldId`: OID of the original object being rebuilt
- `oldRelId`: OID of the relation containing the object
- `refRelId`: OID of the referenced relation (for foreign keys)
- `cmd`: Previously saved definition string to re-parse
- `wqueue`: Double pointer to the ALTER TABLE work queue
- `lockmode`: Lock mode for the operations
- `rewrite`: Whether table rewrite is occurring (affects reuse optimizations)

## Dependencies
- Functions called/Symbols referenced:
  - [raw_parser](../r/raw_parser.md)
  - [transformIndexStmt](../t/transformIndexStmt.md)
  - [transformAlterTableStmt](../t/transformAlterTableStmt.md)
  - [transformStatsStmt](../t/transformStatsStmt.md)
  - [list_concat](../l/list_concat.md)
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - [TryReuseIndex](../T/TryReuseIndex.md)
  - [TryReuseForeignKey](../T/TryReuseForeignKey.md)
  - [GetComment](../G/GetComment.md)
  - [RebuildConstraintComment](../R/RebuildConstraintComment.md)
  - [get_constraint_index](../g/get_constraint_index.md)
  - makeNode
  - castNode
  - Various node types (IndexStmt, AlterTableStmt, CreateStatsStmt, etc.)
- Called from (representative examples):
  - [ATPostAlterTypeCleanup](ATPostAlterTypeCleanup.md) (multiple calls)
  - child_dependency_type

## Notes and Other Information
- Expects only ALTER TABLE and CREATE INDEX statements, bypassing normal query analysis
- Handles different statement types with appropriate transformation functions
- Preserves object comments by retrieving them before recreation
- Uses specialized subtypes (AT_ReAddIndex, AT_ReAddConstraint, etc.) for proper re-creation behavior
- Optimizes by trying to reuse existing objects when possible (TryReuseIndex, TryReuseForeignKey)
- Critical component of PostgreSQL's type change infrastructure for maintaining object consistency