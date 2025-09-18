# ExecuteTruncateGuts

## Location
src/backend/commands/tablecmds.c: 1915 - 2301

## Overview
ExecuteTruncateGuts implements the core TRUNCATE logic, handling the actual data deletion, foreign key cascade processing, sequence restarting, trigger execution, and WAL logging for both direct TRUNCATE commands and logical replication.

## Definition
```c
void ExecuteTruncateGuts(List *explicit_rels, List *relids, List *relids_logged, 
                        DropBehavior behavior, bool restart_seqs, bool run_as_table_owner)
```

## Detailed Description
This function performs the internal implementation of TRUNCATE operations with comprehensive functionality:

1. **CASCADE Processing**: In CASCADE mode, iteratively finds and includes all tables with foreign key references to the target tables, acquiring locks and performing checks on each newly discovered table
2. **Foreign Key Validation**: Validates that all foreign key constraints are satisfied based on the specified behavior (CASCADE/RESTRICT)
3. **Sequence Handling**: When restart_seqs is true, finds all owned sequences and validates permissions before restarting them
4. **Trigger Management**: Sets up executor state and fires BEFORE STATEMENT TRUNCATE triggers before the actual truncation
5. **Table Truncation**: Implements two truncation strategies:
   - **Fast Path**: For tables created in the current subtransaction, uses immediate non-rollbackable truncation
   - **Safe Path**: For existing tables, creates new storage files and schedules old files for deletion at commit
6. **Foreign Table Support**: Groups foreign tables by server and delegates to FDW-specific truncation routines
7. **Index Rebuilding**: Reconstructs indexes after truncation to maintain consistency
8. **WAL Logging**: Creates WAL records for logical decoding when needed
9. **AFTER Trigger Processing**: Fires AFTER STATEMENT TRUNCATE triggers after successful truncation
10. **Cleanup**: Properly closes relations and cleans up resources

The function handles both regular tables and foreign tables, supports inheritance hierarchies, and ensures transactional safety through careful resource management.

## Parameters / Member Variables
- `explicit_rels`: List of Relation objects explicitly specified in the TRUNCATE command
- `relids`: List of OIDs corresponding to explicit_rels
- `relids_logged`: Subset of relids that require WAL logging for logical decoding
- `behavior`: DROP_CASCADE or DROP_RESTRICT behavior for foreign key handling
- `restart_seqs`: Boolean indicating whether to restart sequences owned by truncated tables
- `run_as_table_owner`: Boolean indicating whether triggers should run with table owner privileges

## Dependencies
- Functions called/Symbols referenced:
  - [heap_truncate_find_FKs](../h/heap_truncate_find_FKs.md)
  - [heap_truncate_check_FKs](../h/heap_truncate_check_FKs.md)
  - [truncate_check_rel](../t/truncate_check_rel.md)
  - [truncate_check_perms](../t/truncate_check_perms.md)
  - [truncate_check_activity](../t/truncate_check_activity.md)
  - [getOwnedSequences](../g/getOwnedSequences.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [ExecBSTruncateTriggers](ExecBSTruncateTriggers.md)
  - [ExecASTruncateTriggers](ExecASTruncateTriggers.md)
  - [heap_truncate_one_rel](../h/heap_truncate_one_rel.md)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md)
  - [reindex_relation](../r/reindex_relation.md)
  - [ResetSequence](../R/ResetSequence.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
- Called from (representative examples):
  - [ExecuteTruncate](ExecuteTruncate.md)
  - [apply_handle_truncate](../a/apply_handle_truncate.md)

## Notes and Other Information
- This function is used both by direct TRUNCATE commands and logical replication subscribers
- The function implements PostgreSQL's two-phase truncation approach: fast path for new tables, safe path for existing tables
- Foreign table truncation is delegated to FDW callbacks, allowing different data sources to implement their own truncation logic
- WAL logging is conditional and only occurs when logical decoding is active and relations require it
- The function maintains transactional safety by using subtransaction IDs to determine the appropriate truncation strategy
- Sequence restarting permissions are checked early to avoid partial execution failures
- Trigger execution can optionally run with table owner privileges for security purposes