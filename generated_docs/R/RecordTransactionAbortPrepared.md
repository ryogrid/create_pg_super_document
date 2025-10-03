# RecordTransactionAbortPrepared

## Location
[src/backend/access/transam/twophase.c:2395-2469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L2395-L2469)

## Overview
RecordTransactionAbortPrepared records the abort of a previously prepared two-phase transaction to the Write-Ahead Log and transaction status system, handling cleanup and synchronous replication.

## Definition

```c
static void
RecordTransactionAbortPrepared(TransactionId xid,
							   int nchildren,
							   TransactionId *children,
							   int nrels,
							   RelFileLocator *rels,
							   int nstats,
							   xl_xact_stats_item *stats,
							   const char *gid)
```
## Detailed Description
RecordTransactionAbortPrepared handles the abort processing for a previously prepared two-phase transaction. Similar to its commit counterpart, it writes an abort record to the WAL, marks the transaction and its subtransactions as aborted in the transaction status log (pg_xact), and performs necessary cleanup operations. The function includes safety checks to prevent aborting transactions that have already been committed, which would be a serious consistency violation. It handles replication origins appropriately and ensures synchronous replication requirements are met even for aborted transactions.

## Parameters / Member Variables
- `xid`: The transaction ID of the prepared transaction being aborted
- `nchildren`: Number of subtransactions that are part of this transaction
- `*children`: Array of subtransaction IDs to be aborted along with the main transaction
- `nrels`: Number of relation files associated with this transaction for cleanup
- `*rels`: Array of RelFileLocator structures identifying files related to the transaction
- `nstats`: Number of statistics items associated with this transaction
- `*stats`: Array of statistics items to be processed during abort
- `*gid`: Global transaction identifier string for the prepared transaction being aborted
## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - START_CRIT_SECTION
  - [XactLogAbortRecord](../X/XactLogAbortRecord.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [replorigin_session_advance](../r/replorigin_session_advance.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [TransactionIdAbortTree](../T/TransactionIdAbortTree.md)
  - END_CRIT_SECTION
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md)
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)

## Notes and Other Information
The function performs a critical safety check by verifying the transaction hasn't already been committed before proceeding with the abort, issuing a PANIC if this invariant is violated. Like prepared commits, prepared aborts cannot be optimized out since they always have at least one WAL entry. The function always flushes WAL records before removing the two-phase state file to ensure durability. It operates within a critical section for atomicity and handles both local and replicated transaction scenarios.

## Simplified Source

```c
static void RecordTransactionAbortPrepared(TransactionId xid,
                                          int nchildren, TransactionId *children,
                                          int nrels, RelFileLocator *rels,
                                          int nstats, xl_xact_stats_item *stats,
                                          const char *gid) {
    XLogRecPtr recptr;
    bool replorigin;

    // Check if we're using replication origins (replaying remote actions)
    replorigin = (replorigin_session_origin != InvalidRepOriginId &&
                  replorigin_session_origin != DoNotReplicateId);

    // Safety check: prevent aborting already committed transactions
    if (TransactionIdDidCommit(xid))
        elog(PANIC, "cannot abort transaction %u, it was already committed", xid);

    START_CRIT_SECTION();

    // Write abort record to WAL
    // Mark as potentially having AccessExclusiveLocks (conservative approach)
    recptr = XactLogAbortRecord(GetCurrentTimestamp(),
                               nchildren, children,
                               nrels, rels,
                               nstats, stats,
                               MyXactFlags | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
                               xid, gid);

    // Advance replication origin LSN if using replication
    if (replorigin)
        replorigin_session_advance(replorigin_session_origin_lsn, XactLastRecEnd);

    // Always flush WAL before removing 2PC state file
    XLogFlush(recptr);

    // Mark transaction and subtransactions as aborted in pg_xact
    TransactionIdAbortTree(xid, nchildren, children);

    END_CRIT_SECTION();

    // Wait for synchronous replication if required
    // Note: still holding locks at this point
    SyncRepWaitForLSN(recptr, false);
}
```