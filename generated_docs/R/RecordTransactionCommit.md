# RecordTransactionCommit

## Location
[src/backend/access/transam/xact.c:1304-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1304-L1557)

## Overview
RecordTransactionCommit handles the critical process of recording a transaction's commit to persistent storage, including writing commit records to WAL, managing synchronous/asynchronous commit decisions, and returning the latest transaction ID among the transaction and its children.

## Definition
```c
static TransactionId RecordTransactionCommit(void)
```

## Detailed Description
This function is the core implementation of transaction commit recording in PostgreSQL. It orchestrates the complex process of making a transaction's changes durable by writing appropriate WAL records, managing commit timestamps, handling replication origins, and making decisions about synchronous vs asynchronous commit based on various factors. The function handles both transactions with and without assigned XIDs, manages nested transactions through child XID processing, and coordinates with the checkpoint mechanism to ensure data consistency. It also handles special cases like transactions that only modified temporary tables or performed HOT pruning without requiring full commit processing.

## Parameters / Member Variables
This function takes no parameters but works with numerous local variables:
- : The top-level transaction ID obtained via GetTopTransactionIdIfAny()
- : Boolean indicating if the transaction has a valid XID to commit
- : The most recent XID among the transaction and its children (return value)
- /: Count and array of relation files pending deletion
- /: Count and array of child transaction IDs
- /: Count and array of statistics objects dropped in this transaction
- /: Count and array of invalidation messages for standby servers
- : Flag indicating if relation cache init file invalidation is needed
- : Boolean tracking whether any WAL records were written

## Dependencies
- Functions called/Symbols referenced:
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - XLogLogicalInfoActive/LogLogicalInvalidations
  - [smgrGetPendingDeletes](../s/smgrGetPendingDeletes.md)
  - [xactGetCommittedChildren](../x/xactGetCommittedChildren.md)
  - [pgstat_get_transactional_drops](../p/pgstat_get_transactional_drops.md)
  - XLogStandbyInfoActive/xactGetCommittedInvalidationMessages
  - [LogStandbyInvalidations](../L/LogStandbyInvalidations.md)
  - [XactLogCommitRecord](../X/XactLogCommitRecord.md)
  - [GetCurrentTransactionStopTimestamp](../G/GetCurrentTransactionStopTimestamp.md)
  - [replorigin_session_advance](../r/replorigin_session_advance.md)
  - [TransactionTreeSetCommitTsData](../T/TransactionTreeSetCommitTsData.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [TransactionIdCommitTree](../T/TransactionIdCommitTree.md)
  - [XLogSetAsyncXactLSN](../X/XLogSetAsyncXactLSN.md)
  - [TransactionIdAsyncCommitTree](../T/TransactionIdAsyncCommitTree.md)
  - [TransactionIdLatest](../T/TransactionIdLatest.md)
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)

## Notes and Other Information
- Critical section management prevents checkpoints from interfering with commit processing
- Supports both synchronous and asynchronous commit modes based on configuration and transaction characteristics
- Handles replication origin tracking for logical replication scenarios
- Transactions without XIDs (read-only, temp table only) receive special handling
- The function coordinates with multiple PostgreSQL subsystems: WAL, CLOG, statistics, invalidation messages, and replication
- Returns InvalidTransactionId for transactions without assigned XIDs, otherwise returns the latest XID in the transaction tree
- Essential for PostgreSQL's ACID properties and crash recovery mechanisms

## Simplified Source

```c
static TransactionId RecordTransactionCommit(void)
{
    TransactionId xid = GetTopTransactionIdIfAny();
    bool markXidCommitted = TransactionIdIsValid(xid);
    TransactionId latestXid = InvalidTransactionId;
    int nrels, nchildren, ndroppedstats = 0, nmsgs = 0;
    RelFileLocator *rels;
    TransactionId *children;
    xl_xact_stats_item *droppedstats = NULL;
    SharedInvalidationMessage *invalMessages = NULL;
    bool RelcacheInitFileInval = false;
    bool wrote_xlog;

    // Log pending invalidations for logical decoding
    if (XLogLogicalInfoActive())
        LogLogicalInvalidations();

    // Gather data needed for commit record
    nrels = smgrGetPendingDeletes(true, &rels);
    nchildren = xactGetCommittedChildren(&children);
    ndroppedstats = pgstat_get_transactional_drops(true, &droppedstats);
    if (XLogStandbyInfoActive())
        nmsgs = xactGetCommittedInvalidationMessages(&invalMessages,
                                                     &RelcacheInitFileInval);
    wrote_xlog = (XactLastRecEnd != 0);

    if (!markXidCommitted)
    {
        // Transaction without XID - limited processing
        if (nrels != 0 || ndroppedstats != 0)
            elog(ERROR, "cannot commit a transaction that deleted files but has no xid");

        Assert(nchildren == 0);

        // Handle invalidation messages for standby servers
        if (nmsgs != 0)
        {
            LogStandbyInvalidations(nmsgs, invalMessages, RelcacheInitFileInval);
            wrote_xlog = true;
        }

        if (!wrote_xlog)
            goto cleanup;
    }
    else
    {
        // Transaction with XID - full commit processing
        bool replorigin = (replorigin_session_origin != InvalidRepOriginId &&
                          replorigin_session_origin != DoNotReplicateId);

        // Enter critical section to prevent checkpoint interference
        Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
        START_CRIT_SECTION();
        MyProc->delayChkptFlags |= DELAY_CHKPT_START;

        // Write commit record to WAL
        XactLogCommitRecord(GetCurrentTransactionStopTimestamp(),
                           nchildren, children, nrels, rels,
                           ndroppedstats, droppedstats,
                           nmsgs, invalMessages,
                           RelcacheInitFileInval,
                           MyXactFlags,
                           InvalidTransactionId, NULL);

        // Handle replication origin advancement
        if (replorigin)
            replorigin_session_advance(replorigin_session_origin_lsn,
                                      XactLastRecEnd);

        // Record commit timestamp
        if (!replorigin || replorigin_session_origin_timestamp == 0)
            replorigin_session_origin_timestamp = GetCurrentTransactionStopTimestamp();

        TransactionTreeSetCommitTsData(xid, nchildren, children,
                                      replorigin_session_origin_timestamp,
                                      replorigin_session_origin);
    }

    // Decide between synchronous and asynchronous commit
    if ((wrote_xlog && markXidCommitted &&
         synchronous_commit > SYNCHRONOUS_COMMIT_OFF) ||
        forceSyncCommit || nrels > 0)
    {
        // Synchronous commit path
        XLogFlush(XactLastRecEnd);

        if (markXidCommitted)
            TransactionIdCommitTree(xid, nchildren, children);
    }
    else
    {
        // Asynchronous commit path
        XLogSetAsyncXactLSN(XactLastRecEnd);

        if (markXidCommitted)
            TransactionIdAsyncCommitTree(xid, nchildren, children, XactLastRecEnd);
    }

    // Exit critical section
    if (markXidCommitted)
    {
        MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
        END_CRIT_SECTION();
    }

    // Calculate latest XID for return value
    latestXid = TransactionIdLatest(xid, nchildren, children);

    // Wait for synchronous replication if required
    if (wrote_xlog && markXidCommitted)
        SyncRepWaitForLSN(XactLastRecEnd, true);

    // Update commit tracking variables
    XactLastCommitEnd = XactLastRecEnd;
    XactLastRecEnd = 0;

cleanup:
    // Clean up allocated memory
    if (rels)
        pfree(rels);
    if (ndroppedstats)
        pfree(droppedstats);

    return latestXid;
}
```