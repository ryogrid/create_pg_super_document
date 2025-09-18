# apply_handle_insert

## Location
src/backend/replication/logical/worker.c: 2373 - 2463

## Overview
Handles INSERT messages in PostgreSQL logical replication by processing incoming tuple data and inserting it into the appropriate target relation, with support for partitioned tables and proper security context management.

## Definition
```c
static void apply_handle_insert(StringInfo s)
```

## Detailed Description
This function is the main handler for INSERT replication messages in the logical replication worker process. It performs a complete INSERT operation including security context switching, tuple processing, and proper cleanup. The function handles both regular tables and partitioned tables, routing inserts to the correct partition when necessary.

Key operations include:
1. **Message Processing**: Reads the INSERT message data and extracts the relation ID and tuple data
2. **Security Management**: Switches to the table owner's security context unless the subscription is configured to run as the subscription owner
3. **Tuple Processing**: Converts the remote tuple data into a local tuple slot and fills in default values
4. **Execution**: Either directly inserts into a regular table or routes the insert through the partitioning system for partitioned tables
5. **Resource Management**: Properly manages memory contexts, executor state, and relation locks

The function includes comprehensive error handling and ensures proper cleanup of resources regardless of success or failure.

## Parameters / Member Variables
- `s`: StringInfo buffer containing the INSERT message data from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes
  - handle_streamed_transaction
  - begin_replication_step
  - logicalrep_read_insert
  - logicalrep_rel_open
  - should_apply_changes_for_rel
  - logicalrep_rel_close
  - SwitchToUntrustedUser
  - create_edata_for_relation
  - ExecInitExtraTupleSlot
  - GetPerTupleMemoryContext
  - slot_store_data
  - slot_fill_defaults
  - apply_handle_tuple_routing
  - apply_handle_insert_internal
  - ExecOpenIndices
  - ExecCloseIndices
  - finish_edata
  - RestoreUserContext
  - end_replication_step
  - LogicalRepRelMapEntry (data structure)
  - LogicalRepTupleData (data structure)
  - UserContext (data structure)
  - ApplyExecutionData (data structure)
  - LOGICAL_REP_MSG_INSERT (constant)
  - CMD_INSERT (constant)
- Called from (representative examples):
  - apply_dispatch

## Notes and Other Information
- This is a static function within the logical replication worker module
- Supports both regular and partitioned tables with appropriate routing logic
- Implements security context switching based on subscription configuration (runasowner setting)
- Uses PostgreSQL's executor framework for tuple processing and insertion
- Includes comprehensive resource management with proper memory context handling
- Part of the core logical replication message processing pipeline
- Handles early returns for skipped changes and streamed transactions
- Maintains error callback context for better error reporting during replication