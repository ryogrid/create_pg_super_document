# ExecCrossPartitionUpdate

## Location
src/backend/executor/nodeModifyTable.c: 1763 - 1923

## Overview
Moves an updated tuple to a different partition by performing a coordinated delete-from-old-partition and insert-into-new-partition operation.

## Definition


## Detailed Description
ExecCrossPartitionUpdate handles the complex scenario where an UPDATE operation changes a tuple's partition key, requiring the tuple to be moved from one partition to another. This operation cannot be performed as a simple in-place update because it spans partition boundaries.

The function implements a two-phase approach:
1. **Delete Phase**: Removes the tuple from the current partition using ExecDelete
2. **Insert Phase**: Inserts the modified tuple into the root table, allowing tuple routing to direct it to the correct destination partition

Key features include:
- **Concurrency Handling**: Manages concurrent modifications during the cross-partition move
- **Constraint Checking**: Validates partition constraints and prevents unsupported operations
- **Tuple Conversion**: Converts tuple format between child partition and root table schemas
- **Transaction Semantics**: Maintains ACID properties across the delete-insert sequence
- **Retry Logic**: Provides mechanisms to handle concurrent updates that may require operation retry

## Parameters / Member Variables
- : ModifyTableContext containing execution state and metadata
- : Information about the source partition relation
- : ItemPointer identifying the tuple to be moved
- : HeapTuple containing the original tuple data
- : TupleTableSlot containing the new tuple data after update
- : Boolean controlling whether to increment processed tuple count
- : UpdateContext containing update-specific execution state
- : Output parameter receiving the tuple modification result
- : Output parameter returning tuple data if retry is needed due to concurrent modification
- : Output parameter returning the inserted tuple slot
- : Output parameter returning the destination relation info for the insert

## Dependencies
- Functions called/Symbols referenced:
  - ExecDelete (removes tuple from current partition)
  - ExecInsert (inserts tuple into root table for re-routing)
  - ExecSetupPartitionTupleRouting (initializes partition routing infrastructure)
  - ExecGetChildToRootMap (obtains tuple conversion map)
  - execute_attr_map_slot (converts tuple between schemas)
  - ExecPartitionCheckEmitError (validates partition constraints)
  - ExecGetUpdateNewTuple (generates new tuple for retry scenarios)
- Called from:
  - ExecUpdateAct (when update requires cross-partition movement)

## Notes and Other Information
- Returns true if the move was successful or if the tuple was concurrently deleted
- Returns false if concurrent update occurred and retry is needed (retry_slot contains updated data)
- Explicitly disallows INSERT ON CONFLICT DO UPDATE operations that would cause partition movement
- Fails with partition constraint violation if UPDATE is run directly on a leaf partition
- Initializes partition tuple routing infrastructure lazily on first use
- Manages transition capture state to handle statement-level triggers properly
- Uses root table as the entry point for tuple routing to find the correct destination partition
- Part of PostgreSQL's partitioned table update mechanism