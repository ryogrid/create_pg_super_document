# RelationFindReplTupleSeq

## Location
[src/backend/executor/execReplication.c:378-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L378-L489)

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
  - [ExecCopySlot](../E/ExecCopySlot.md): Copies tuple data between slots
  - [table_tuple_lock](../t/table_tuple_lock.md): Locks the found tuple
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md): Gets current command ID for locking
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md): Cleans up scan slot
- Called from (representative examples):
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md): Logical replication worker function
  - [exec_rt_fetch](../e/exec_rt_fetch.md): Through executor header inclusion

## Notes and Other Information
- Uses a retry mechanism with goto statements to handle concurrent modifications
- Performance warning: Can be slow on large tables due to sequential scanning
- Implements comprehensive error handling for various tuple locking scenarios
- Designed specifically for replication scenarios where exact tuple matching is critical
- Uses dirty snapshots for scanning but latest snapshots for locking to ensure consistency
- Handles partition tuple movements and concurrent updates with appropriate logging

## Simplified Source

```c
bool RelationFindReplTupleSeq(Relation rel, LockTupleMode lockmode,
                              TupleTableSlot *searchslot, TupleTableSlot *outslot) {
    TableScanDesc scan;
    TupleTableSlot *scanslot;
    SnapshotData snap;
    TypeCacheEntry **eq;
    bool found;

    // Initialize equality comparison cache and start sequential scan
    eq = palloc0(sizeof(*eq) * outslot->tts_tupleDescriptor->natts);
    InitDirtySnapshot(snap);
    scan = table_beginscan(rel, &snap, 0, NULL);
    scanslot = table_slot_create(rel, NULL);

retry:
    found = false;
    table_rescan(scan, NULL);

    // Sequential scan through all tuples
    while (table_scan_getnextslot(scan, ForwardScanDirection, scanslot)) {
        // Check if this tuple matches our search criteria
        if (!tuples_equal(scanslot, searchslot, eq))
            continue;

        found = true;
        ExecCopySlot(outslot, scanslot);

        // Handle concurrent transactions
        TransactionId xwait = TransactionIdIsValid(snap.xmin) ? snap.xmin : snap.xmax;
        if (TransactionIdIsValid(xwait)) {
            XactLockTableWait(xwait, NULL, NULL, XLTW_None);
            goto retry;
        }

        break;
    }

    // Lock the found tuple
    if (found) {
        TM_FailureData tmfd;
        PushActiveSnapshot(GetLatestSnapshot());

        TM_Result res = table_tuple_lock(rel, &(outslot->tts_tid), GetActiveSnapshot(),
                                        outslot, GetCurrentCommandId(false), lockmode,
                                        LockWaitBlock, 0, &tmfd);

        PopActiveSnapshot();

        // Handle concurrent modifications
        switch (res) {
            case TM_Ok:
                break;
            case TM_Updated:
            case TM_Deleted:
                // Log and retry for concurrent changes
                ereport(LOG, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                             errmsg("concurrent modification, retrying")));
                goto retry;
            case TM_Invisible:
                elog(ERROR, "attempted to lock invisible tuple");
                break;
            default:
                elog(ERROR, "unexpected table_tuple_lock status: %u", res);
                break;
        }
    }

    table_endscan(scan);
    ExecDropSingleTupleTableSlot(scanslot);
    return found;
}
```