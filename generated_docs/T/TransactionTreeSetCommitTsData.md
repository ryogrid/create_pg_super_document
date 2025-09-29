# TransactionTreeSetCommitTsData

## Location
[src/backend/access/transam/commit_ts.c:141-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L141-L221)

## Overview
Records the final commit timestamp for a transaction and its entire subtransaction tree, optimizing storage by grouping transactions by SLRU page to minimize page locks.

## Definition
```c
void TransactionTreeSetCommitTsData(TransactionId xid, int nsubxids,
                                   TransactionId *subxids, TimestampTz timestamp,
                                   RepOriginId nodeid)
```

## Detailed Description
This function efficiently records commit timestamp data for a main transaction and all its subtransactions. The key optimization is that it groups transactions by SLRU page to minimize the number of page locks required. Instead of locking each page multiple times, it processes all transactions belonging to the same page in a single operation.

The function handles the complexity of subtransaction trees by storing timestamp information for each subtransaction individually, since the subtrans SLRU is not persistent across crashes. This ensures that commit timestamp information remains available even after system restarts.

The algorithm splits the transaction IDs into groups where each group contains transactions that belong to the same SLRU page. It then processes each group by calling SetXidCommitTsInPage once per page, significantly reducing lock contention and improving performance.

## Parameters / Member Variables
- `xid`: The main/parent transaction ID
- `nsubxids`: Number of subtransactions in the subxids array
- `subxids`: Array of subtransaction IDs (may be NULL if nsubxids is 0)
- `timestamp`: The commit timestamp to record for all transactions
- `nodeid`: Replication origin ID associated with this commit

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdToCTsPage](TransactionIdToCTsPage.md) (to determine which page contains each transaction)
  - [SetXidCommitTsInPage](../S/SetXidCommitTsInPage.md) (to actually write timestamp data to a page)
  - [TransactionIdPrecedes](TransactionIdPrecedes.md) (to compare transaction ID ordering)
  - RepOriginId (type for replication origin identification)
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md) (during normal transaction commit)
  - [RecordTransactionCommitPrepared](../R/RecordTransactionCommitPrepared.md) (during two-phase commit)
  - [xact_redo_commit](../x/xact_redo_commit.md) (during WAL replay)

## Notes and Other Information
- Returns early if commit timestamp tracking is not active (commitTsShared->commitTsActive is false)
- Uses an efficient batching algorithm to minimize SLRU page locks
- Updates cached values in shared memory for the last committed transaction
- Advances the newestCommitTsXid marker if this commit represents the newest transaction
- The function is designed to handle cases where nsubxids is zero (no subtransactions)
- Critical for maintaining commit timestamp consistency across transaction trees
- Location: src/backend/access/transam/commit_ts.c:141-221

## Simplified Source

```c
void TransactionTreeSetCommitTsData(TransactionId xid, int nsubxids,
                                   TransactionId *subxids, TimestampTz timestamp,
                                   RepOriginId nodeid) {
    // Skip if commit timestamp tracking is disabled
    if (!commitTsShared->commitTsActive)
        return;

    // Find the newest transaction ID in this batch
    TransactionId newestXact = (nsubxids > 0) ? subxids[nsubxids - 1] : xid;

    // Process transactions in groups by SLRU page to minimize page locks
    TransactionId headxid = xid;
    int i = 0;

    while (true) {
        int64 pageno = TransactionIdToCTsPage(headxid);
        int j;

        // Find all subxids on the same page as headxid
        for (j = i; j < nsubxids; j++) {
            if (TransactionIdToCTsPage(subxids[j]) != pageno)
                break;
        }

        // Set commit timestamp for all transactions on this page
        SetXidCommitTsInPage(headxid, j - i, subxids + i, timestamp, nodeid, pageno);

        // Exit if we've processed all subtransactions
        if (j >= nsubxids)
            break;

        // Move to next page group
        headxid = subxids[j];
        i = j + 1;
    }

    // Update shared memory cache with latest commit info
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    commitTsShared->xidLastCommit = xid;
    commitTsShared->dataLastCommit.time = timestamp;
    commitTsShared->dataLastCommit.nodeid = nodeid;

    // Advance newest commit timestamp marker if needed
    if (TransactionIdPrecedes(TransamVariables->newestCommitTsXid, newestXact))
        TransamVariables->newestCommitTsXid = newestXact;

    LWLockRelease(CommitTsLock);
}
```