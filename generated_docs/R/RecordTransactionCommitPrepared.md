# RecordTransactionCommitPrepared

## Location
[src/backend/access/transam/twophase.c:2297-2394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L2297-L2394)

## Overview
RecordTransactionCommitPrepared records the commit of a previously prepared two-phase transaction to the Write-Ahead Log and transaction status system, handling replication origins and synchronous replication.

## Definition

```c
static void
RecordTransactionCommitPrepared(TransactionId xid,
								int nchildren,
								TransactionId *children,
								int nrels,
								RelFileLocator *rels,
								int nstats,
								xl_xact_stats_item *stats,
								int ninvalmsgs,
								SharedInvalidationMessage *invalmsgs,
								bool initfileinval,
								const char *gid)
```
## Detailed Description
RecordTransactionCommitPrepared is the final stage function for committing a prepared two-phase transaction. It writes a commit record to the WAL, marks the transaction as committed in the transaction status log (pg_xact), and handles all associated cleanup including relation file deletions, cache invalidation messages, and statistics updates. The function follows similar patterns to regular transaction commits but is specifically designed for two-phase transactions that have already been prepared. It includes special handling for replication origins when PostgreSQL is acting as a logical replication subscriber, and ensures proper synchronization with checkpointing and synchronous replication.

## Parameters / Member Variables
- `xid`: The transaction ID of the prepared transaction being committed
- `nchildren`: Number of subtransactions involved in this transaction
- `*children`: Array of subtransaction IDs that are part of this transaction
- `nrels`: Number of relation files to be deleted as part of this commit
- `*rels`: Array of RelFileLocator structures identifying files to delete
- `nstats`: Number of statistics items to update
- `*stats`: Array of statistics items to be updated in system catalogs
- `ninvalmsgs`: Number of shared invalidation messages to process
- `*invalmsgs`: Array of shared invalidation messages for cache consistency
- `initfileinval`: Boolean indicating whether to invalidate init files
- `*gid`: Global transaction identifier string for the prepared transaction
## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - START_CRIT_SECTION
  - [XactLogCommitRecord](../X/XactLogCommitRecord.md)
  - [replorigin_session_advance](../r/replorigin_session_advance.md)
  - [TransactionTreeSetCommitTsData](../T/TransactionTreeSetCommitTsData.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [TransactionIdCommitTree](../T/TransactionIdCommitTree.md)
  - END_CRIT_SECTION
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md)
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)

## Notes and Other Information
The function operates within a critical section to ensure atomicity and uses checkpoint delay flags to prevent race conditions during commit processing. Unlike regular commits, prepared transaction commits cannot be optimized out since they always have at least one WAL entry (the PREPARE record). The function handles both local and replicated transactions, managing commit timestamps and replication origin advancement appropriately.

## Simplified Source

```c
static void RecordTransactionCommitPrepared(TransactionId xid,
                                           int nchildren, TransactionId *children,
                                           int nrels, RelFileLocator *rels,
                                           int nstats, xl_xact_stats_item *stats,
                                           int ninvalmsgs, SharedInvalidationMessage *invalmsgs,
                                           bool initfileinval, const char *gid) {
    XLogRecPtr recptr;
    TimestampTz committs = GetCurrentTimestamp();
    bool replorigin;

    // Check if we're using replication origins (replaying remote actions)
    replorigin = (replorigin_session_origin != InvalidRepOriginId &&
                  replorigin_session_origin != DoNotReplicateId);

    START_CRIT_SECTION();

    // Delay checkpoint start to prevent race conditions
    Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
    MyProc->delayChkptFlags |= DELAY_CHKPT_START;

    // Write commit record to WAL
    // Mark as potentially having AccessExclusiveLocks (conservative approach)
    recptr = XactLogCommitRecord(committs,
                                nchildren, children, nrels, rels,
                                nstats, stats,
                                ninvalmsgs, invalmsgs,
                                initfileinval,
                                MyXactFlags | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
                                xid, gid);

    // Advance replication origin LSN if using replication
    if (replorigin)
        replorigin_session_advance(replorigin_session_origin_lsn, XactLastRecEnd);

    // Record commit timestamp
    if (!replorigin || replorigin_session_origin_timestamp == 0)
        replorigin_session_origin_timestamp = committs;

    TransactionTreeSetCommitTsData(xid, nchildren, children,
                                  replorigin_session_origin_timestamp,
                                  replorigin_session_origin);

    // Flush WAL to ensure durability
    XLogFlush(recptr);

    // Mark transaction and subtransactions as committed in pg_xact
    TransactionIdCommitTree(xid, nchildren, children);

    // Allow checkpoint to proceed
    MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;

    END_CRIT_SECTION();

    // Wait for synchronous replication if required
    // Note: still holding locks at this point
    SyncRepWaitForLSN(recptr, true);
}
```