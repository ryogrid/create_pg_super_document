# RelationFindReplTupleSeq

## Location
src/backend/executor/execReplication.c: 378 - 489

## Overview
RelationFindReplTupleSeq performs a sequential scan search on a relation to find a tuple matching search criteria, locks it if found, and fills an output slot with the tuple contents.

## Definition
```c
bool RelationFindReplTupleSeq(Relation rel, LockTupleMode lockmode,
                             TupleTableSlot *searchslot, TupleTableSlot *outslot)
```

## Detailed Description
This function searches through a relation using a sequential scan to find the first tuple that matches the contents of a search slot. It is primarily used in logical replication scenarios where exact tuple matching is needed. The function implements a retry mechanism to handle concurrent modifications - if the target tuple is locked by another transaction, it waits for that transaction to complete and retries the operation.

When a matching tuple is found, the function attempts to lock it with the specified lock mode. It handles various concurrent scenarios including tuple updates, deletes, and partition movements by logging appropriate messages and retrying the operation. The function uses a dirty snapshot for the initial scan and switches to the latest snapshot when attempting to lock the found tuple.

Note that this approach can be quite slow on large tables since it performs a full sequential scan, but it provides reliable tuple identification for replication purposes.

## Parameters / Member Variables
- `rel`: The relation to search in
- `lockmode`: The lock mode to acquire on the found tuple (e.g., LockTupleExclusive, LockTupleShare)
- `searchslot`: TupleTableSlot containing the tuple values to search for
- `outslot`: TupleTableSlot to fill with the contents of the found tuple

## Dependencies
- Functions called/Symbols referenced:
  - [equalTupleDescs](../e/equalTupleDescs.md): Validates tuple descriptor compatibility
  - InitDirtySnapshot: Initializes snapshot for scanning
  - [table_beginscan](../t/table_beginscan.md): Starts table scan
  - [table_slot_create](../t/table_slot_create.md): Creates scan slot
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md): Gets next tuple from scan
  - [tuples_equal](../t/tuples_equal.md): Compares tuples for equality
  - ExecCopySlot: Copies tuple data between slots
  - table_tuple_lock: Locks the found tuple
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md): Gets current command ID for locking
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md): Cleans up scan slot
- Called from (representative examples):
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md): Logical replication worker function
  - exec_rt_fetch: Through executor header inclusion

## Notes and Other Information
- Uses a retry mechanism with goto statements to handle concurrent modifications
- Performance warning: Can be slow on large tables due to sequential scanning
- Implements comprehensive error handling for various tuple locking scenarios
- Designed specifically for replication scenarios where exact tuple matching is critical
- Uses dirty snapshots for scanning but latest snapshots for locking to ensure consistency
- Handles partition tuple movements and concurrent updates with appropriate logging