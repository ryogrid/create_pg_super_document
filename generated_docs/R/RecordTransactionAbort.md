# RecordTransactionAbort

## Location
[src/backend/access/transam/xact.c:1723-1852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1723-L1852)

## Overview
RecordTransactionAbort handles the recording of transaction abort operations by writing abort records to WAL, marking transactions as aborted in the commit log, and cleaning up associated resources.

## Definition

```c
static TransactionId
RecordTransactionAbort(bool isSubXact)
```
## Detailed Description
This function orchestrates the complete process of recording a transaction abort in PostgreSQL's transaction management system. It writes an ABORT record to the Write-Ahead Log (WAL), marks the transaction and its children as aborted in the commit log (clog), and performs necessary cleanup operations.

The function first verifies that the transaction has a valid XID - if not, no abort record is needed since unassigned transactions don't affect system state. For valid transactions, it ensures the transaction hasn't already been committed (which would be a PANIC condition), then proceeds to collect information about child transactions, pending file deletions, and dropped statistics.

The abort record creation occurs within a critical section to ensure atomicity. For main transactions, it uses the transaction stop timestamp, while subtransactions use the current timestamp. The function also handles replication origin advancement and async abort LSN reporting for WAL writer coordination.

After recording the abort, it marks the transaction tree as aborted in clog and, for subtransactions, immediately removes failed XIDs from the running transaction cache. Finally, it computes and returns the latest XID among the transaction and its children.

## Parameters / Member Variables
- `isSubXact`: Boolean flag indicating whether this is a subtransaction (true) or main transaction (false) abort
## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionIdIfAny](../G/GetCurrentTransactionIdIfAny.md) (check for valid transaction ID)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md) (verify transaction not already committed)
  - [smgrGetPendingDeletes](../s/smgrGetPendingDeletes.md) (fetch pending file deletions)
  - [xactGetCommittedChildren](../x/xactGetCommittedChildren.md) (retrieve committed child transactions)
  - [pgstat_get_transactional_drops](../p/pgstat_get_transactional_drops.md) (get dropped statistics)
  - [XactLogAbortRecord](../X/XactLogAbortRecord.md) (write abort record to WAL)
  - [TransactionIdAbortTree](../T/TransactionIdAbortTree.md) (mark transaction tree as aborted in clog)
  - [TransactionIdLatest](../T/TransactionIdLatest.md) (compute latest XID in transaction tree)
  - [XidCacheRemoveRunningXids](../X/XidCacheRemoveRunningXids.md) (remove failed XIDs from cache)
  - [GetCurrentTransactionStopTimestamp](../G/GetCurrentTransactionStopTimestamp.md)/GetCurrentTimestamp (timestamp handling)
- Called from:
  - [AbortTransaction](../A/AbortTransaction.md) (main transaction abort at src/backend/access/transam/xact.c:2871)
  - [AbortSubTransaction](../A/AbortSubTransaction.md) (subtransaction abort at src/backend/access/transam/xact.c:5267)

## Notes and Other Information
- Returns the latest XID among the transaction and its children, or InvalidTransactionId if no XID was assigned
- Does not flush WAL to disk immediately since abort is the default assumption after crash
- Uses critical sections to ensure atomicity of abort record writing and clog updates
- Handles replication origin advancement for logical replication scenarios
- For subtransactions, immediately cleans up the running XID cache for performance
- Includes comprehensive error checking to prevent aborting already-committed transactions
- Manages cleanup of temporary data structures allocated during the abort process

## Simplified Source

```c
// Simplified version of RecordTransactionAbort
static TransactionId
RecordTransactionAbort(bool isSubXact)
{
    TransactionId xid = GetCurrentTransactionIdIfAny();
    TransactionId latestXid;
    int nrels, nchildren, ndroppedstats;
    RelFileLocator *rels;
    TransactionId *children;
    xl_xact_stats_item *droppedstats;
    TimestampTz xact_time;
    bool replorigin;

    // Early exit if no XID assigned - nothing to abort
    if (!TransactionIdIsValid(xid)) {
        if (!isSubXact)
            XactLastRecEnd = 0;
        return InvalidTransactionId;
    }

    // Safety check: ensure transaction wasn't already committed
    if (TransactionIdDidCommit(xid))
        elog(PANIC, "cannot abort transaction %u, it was already committed", xid);

    // Check if we're in replication mode
    replorigin = (replorigin_session_origin != InvalidRepOriginId &&
                  replorigin_session_origin != DoNotReplicateId);

    // Collect abort-related data
    nrels = smgrGetPendingDeletes(false, &rels);
    nchildren = xactGetCommittedChildren(&children);
    ndroppedstats = pgstat_get_transactional_drops(false, &droppedstats);

    START_CRIT_SECTION();

    // Set appropriate timestamp for abort record
    if (isSubXact)
        xact_time = GetCurrentTimestamp();
    else
        xact_time = GetCurrentTransactionStopTimestamp();

    // Write the abort record to WAL
    XactLogAbortRecord(xact_time, nchildren, children, nrels, rels,
                       ndroppedstats, droppedstats, MyXactFlags,
                       InvalidTransactionId, NULL);

    // Advance replication origin if needed
    if (replorigin)
        replorigin_session_advance(replorigin_session_origin_lsn, XactLastRecEnd);

    // Set async abort LSN for WAL writer
    if (!isSubXact)
        XLogSetAsyncXactLSN(XactLastRecEnd);

    // Mark transaction and children as aborted in commit log
    TransactionIdAbortTree(xid, nchildren, children);

    END_CRIT_SECTION();

    // Calculate latest XID in the transaction tree
    latestXid = TransactionIdLatest(xid, nchildren, children);

    // For subtransactions, clean up running XID cache immediately
    if (isSubXact)
        XidCacheRemoveRunningXids(xid, nchildren, children, latestXid);

    // Reset transaction state for main transactions
    if (!isSubXact)
        XactLastRecEnd = 0;

    // Clean up allocated memory
    if (rels)
        pfree(rels);
    if (ndroppedstats)
        pfree(droppedstats);

    return latestXid;
}
```

Key simplifications made:
- Removed detailed comments and consolidated variable declarations
- Simplified replication origin check logic
- Streamlined critical section operations with clearer flow
- Combined related cleanup operations
- Abstracted complex condition checks into simpler boolean expressions
- Maintained all essential functionality while improving readability