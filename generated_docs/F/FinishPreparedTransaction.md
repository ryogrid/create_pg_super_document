# FinishPreparedTransaction

## Location
[src/backend/access/transam/twophase.c:1487-1679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1487-L1679)

## Overview
FinishPreparedTransaction executes the final phase of a two-phase commit, handling both COMMIT PREPARED and ROLLBACK PREPARED operations to complete prepared transactions.

## Definition
void FinishPreparedTransaction(const char *gid, bool isCommit)

## Detailed Description
This function performs the complete finalization of a prepared transaction identified by its Global Identifier (GID). It orchestrates the complex sequence of operations required to either commit or rollback a prepared transaction, including WAL logging, shared memory cleanup, file operations, cache invalidation, and callback execution. The function maintains strict ordering of operations to ensure data consistency: first logging the transaction outcome, then updating transaction status, removing the transaction from the process array, and finally executing post-commit or post-abort callbacks. It handles both on-disk and in-WAL stored transaction state data, manages relation file drops, executes statistics updates, and processes cache invalidation messages.

## Parameters / Member Variables
- `gid`: The Global Identifier string that uniquely identifies the prepared transaction to finish
- `isCommit`: Boolean flag indicating whether to commit (true) or rollback (false) the transaction

## Dependencies
- Functions called/Symbols referenced:
  - [LockGXact](../L/LockGXact.md)
  - [ReadTwoPhaseFile](../R/ReadTwoPhaseFile.md)
  - [XlogReadTwoPhaseData](../X/XlogReadTwoPhaseData.md)
  - [TransactionIdLatest](../T/TransactionIdLatest.md)
  - [RecordTransactionCommitPrepared](../R/RecordTransactionCommitPrepared.md)
  - [RecordTransactionAbortPrepared](../R/RecordTransactionAbortPrepared.md)
  - [ProcArrayRemove](../P/ProcArrayRemove.md)
  - [ProcessRecords](../P/ProcessRecords.md)
  - [RemoveTwoPhaseFile](../R/RemoveTwoPhaseFile.md)
  - [DropRelationFiles](../D/DropRelationFiles.md)
  - [SendSharedInvalidMessages](../S/SendSharedInvalidMessages.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (for COMMIT/ROLLBACK PREPARED statements)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md)
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md)

## Notes and Other Information
- Uses critical sections with HOLD_INTERRUPTS/RESUME_INTERRUPTS to prevent interruption during cleanup
- Maintains strict operation ordering for consistency: WAL logging → transaction status update → process array removal → callbacks
- Handles both on-disk stored state (via ReadTwoPhaseFile) and WAL-stored state (via XlogReadTwoPhaseData)
- Processes relation cache invalidation messages only for commits, with pre/post invalidation phases
- Manages automatic cleanup of relation files that should be dropped as part of the transaction
- Acquires TwoPhaseStateLock during callback processing to prevent conflicts with other transactions

## Simplified Source

```c
void FinishPreparedTransaction(const char *gid, bool isCommit)
{
    GlobalTransaction gxact;
    PGPROC *proc;
    TransactionId xid;
    bool ondisk;
    char *buf;
    char *bufptr;
    TwoPhaseFileHeader *hdr;
    TransactionId latestXid;
    TransactionId *children;
    RelFileLocator *commitrels;
    RelFileLocator *abortrels;
    RelFileLocator *delrels;
    int ndelrels;
    xl_xact_stats_item *commitstats;
    xl_xact_stats_item *abortstats;
    SharedInvalidationMessage *invalmsgs;

    // Lock the global transaction for this GID
    gxact = LockGXact(gid, GetUserId());
    proc = GetPGProcByNumber(gxact->pgprocno);
    xid = gxact->xid;

    // Read 2PC state data (from disk or WAL)
    if (gxact->ondisk)
        buf = ReadTwoPhaseFile(xid, false);
    else
        XlogReadTwoPhaseData(gxact->prepare_start_lsn, &buf, NULL);

    // Parse the 2PC state data header and components
    hdr = (TwoPhaseFileHeader *) buf;
    Assert(TransactionIdEquals(hdr->xid, xid));

    bufptr = buf + MAXALIGN(sizeof(TwoPhaseFileHeader));
    bufptr += MAXALIGN(hdr->gidlen);
    children = (TransactionId *) bufptr;
    bufptr += MAXALIGN(hdr->nsubxacts * sizeof(TransactionId));
    commitrels = (RelFileLocator *) bufptr;
    bufptr += MAXALIGN(hdr->ncommitrels * sizeof(RelFileLocator));
    abortrels = (RelFileLocator *) bufptr;
    bufptr += MAXALIGN(hdr->nabortrels * sizeof(RelFileLocator));
    commitstats = (xl_xact_stats_item *) bufptr;
    bufptr += MAXALIGN(hdr->ncommitstats * sizeof(xl_xact_stats_item));
    abortstats = (xl_xact_stats_item *) bufptr;
    bufptr += MAXALIGN(hdr->nabortstats * sizeof(xl_xact_stats_item));
    invalmsgs = (SharedInvalidationMessage *) bufptr;

    // Find latest XID among all child transactions
    latestXid = TransactionIdLatest(xid, hdr->nsubxacts, children);

    // Critical section: prevent interrupts during cleanup
    HOLD_INTERRUPTS();

    // Step 1: Write commit/abort WAL record
    if (isCommit)
        RecordTransactionCommitPrepared(xid, hdr->nsubxacts, children,
                                       hdr->ncommitrels, commitrels,
                                       hdr->ncommitstats, commitstats,
                                       hdr->ninvalmsgs, invalmsgs,
                                       hdr->initfileinval, gid);
    else
        RecordTransactionAbortPrepared(xid, hdr->nsubxacts, children,
                                      hdr->nabortrels, abortrels,
                                      hdr->nabortstats, abortstats, gid);

    // Step 2: Remove from process array (makes transaction no longer "in progress")
    ProcArrayRemove(proc, latestXid);

    // Step 3: Mark transaction invalid for safety
    gxact->valid = false;

    // Step 4: Drop relation files that should be dropped
    if (isCommit)
    {
        delrels = commitrels;
        ndelrels = hdr->ncommitrels;
    }
    else
    {
        delrels = abortrels;
        ndelrels = hdr->nabortrels;
    }

    DropRelationFiles(delrels, ndelrels, false);

    // Step 5: Execute statistics drops
    if (isCommit)
        pgstat_execute_transactional_drops(hdr->ncommitstats, commitstats, false);
    else
        pgstat_execute_transactional_drops(hdr->nabortstats, abortstats, false);

    // Step 6: Handle cache invalidation (only for commits)
    if (isCommit)
    {
        if (hdr->initfileinval)
            RelationCacheInitFilePreInvalidate();
        SendSharedInvalidMessages(invalmsgs, hdr->ninvalmsgs);
        if (hdr->initfileinval)
            RelationCacheInitFilePostInvalidate();
    }

    // Step 7: Execute callbacks while holding two-phase lock
    LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);

    if (isCommit)
        ProcessRecords(bufptr, xid, twophase_postcommit_callbacks);
    else
        ProcessRecords(bufptr, xid, twophase_postabort_callbacks);

    PredicateLockTwoPhaseFinish(xid, isCommit);

    // Clean up shared memory state
    ondisk = gxact->ondisk;
    RemoveGXact(gxact);

    LWLockRelease(TwoPhaseStateLock);

    // Final cleanup
    AtEOXact_PgStat(isCommit, false);

    if (ondisk)
        RemoveTwoPhaseFile(xid, true);

    MyLockedGxact = NULL;
    RESUME_INTERRUPTS();
    pfree(buf);
}
```