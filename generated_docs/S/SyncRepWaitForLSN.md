# SyncRepWaitForLSN

## Location
[src/backend/replication/syncrep.c:148-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L148-L371)

## Overview
Waits for synchronous replication confirmation for a specific Log Sequence Number (LSN), implementing the core blocking mechanism that ensures transaction commits are replicated to standby servers before acknowledging success to the client.

## Definition
```c
void SyncRepWaitForLSN(XLogRecPtr lsn, bool commit)
```

## Detailed Description
This function implements the waiting mechanism for synchronous replication in PostgreSQL. When synchronous replication is enabled, backends must wait for confirmation that their WAL records have been replicated to standby servers before completing transaction commits. The function manages state transitions from SYNC_REP_NOT_WAITING to SYNC_REP_WAITING, adds the process to a wait queue, and blocks using latches until WAL senders confirm replication.

The function performs several optimizations including fast exits when sync replication is not requested, LSN-based early returns when replication has already been confirmed, and careful lock management to avoid race conditions. It handles interrupts gracefully, issuing warnings when processes must be terminated while waiting for replication confirmation.

For non-commit records, the function caps the synchronization level to remote flush only, since apply feedback is only available for commit records. The waiting process updates its ps display to show the LSN being waited for, providing visibility into replication status.

## Parameters / Member Variables
- `lsn`: The Log Sequence Number to wait for replication confirmation
- `commit`: Boolean indicating whether this LSN represents a commit record (affects synchronization level)

## Dependencies
- Functions called/Symbols referenced:
  - SyncRepRequested
  - [SyncRepQueueInsert](SyncRepQueueInsert.md)
  - [SyncRepQueueIsOrderedByLSN](SyncRepQueueIsOrderedByLSN.md)
  - [SyncRepCancelWait](SyncRepCancelWait.md)
  - SyncStandbysDefined
  - [WaitLatch](../W/WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - [set_ps_display_suffix](../s/set_ps_display_suffix.md)
  - [set_ps_display_remove_suffix](../s/set_ps_display_remove_suffix.md)
  - pg_read_barrier
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [RecordTransactionCommitPrepared](../R/RecordTransactionCommitPrepared.md)
  - [RecordTransactionAbortPrepared](../R/RecordTransactionAbortPrepared.md)
  - [EndPrepare](../E/EndPrepare.md)

## Notes and Other Information
This function must be called while holding interrupts during transaction commit to prevent shared memory queue cleanups from being influenced by external interruptions. The function implements careful state management and uses memory barriers to ensure proper synchronization between WAL senders and waiting backends. It gracefully handles process termination requests and query cancellations by issuing appropriate warnings about potentially unreplicated transactions.

## Simplified Source

```c
void
SyncRepWaitForLSN(XLogRecPtr lsn, bool commit)
{
    int mode;

    // Must be called during interrupt holdoff
    Assert(InterruptHoldoffCount > 0);

    // Fast exit if sync replication not requested or no standbys defined
    if (!SyncRepRequested() ||
        ((((volatile WalSndCtlData *) WalSndCtl)->sync_standbys_status) &
         (SYNC_STANDBY_INIT | SYNC_STANDBY_DEFINED)) == SYNC_STANDBY_INIT)
        return;

    // Set replication mode (cap non-commits to flush level)
    if (commit)
        mode = SyncRepWaitMode;
    else
        mode = Min(SyncRepWaitMode, SYNC_REP_WAIT_FLUSH);

    Assert(dlist_node_is_detached(&MyProc->syncRepLinks));
    Assert(WalSndCtl != NULL);

    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
    Assert(MyProc->syncRepState == SYNC_REP_NOT_WAITING);

    // Check if we need to wait based on standby status and LSN
    if (WalSndCtl->sync_standbys_status & SYNC_STANDBY_INIT)
    {
        if ((WalSndCtl->sync_standbys_status & SYNC_STANDBY_DEFINED) == 0 ||
            lsn <= WalSndCtl->lsn[mode])
        {
            LWLockRelease(SyncRepLock);
            return;
        }
    }
    else if (lsn <= WalSndCtl->lsn[mode])
    {
        // LSN already replicated
        LWLockRelease(SyncRepLock);
        return;
    }
    else if (!SyncStandbysDefined())
    {
        // No sync standbys configured
        LWLockRelease(SyncRepLock);
        return;
    }

    // Add ourselves to wait queue
    MyProc->waitLSN = lsn;
    MyProc->syncRepState = SYNC_REP_WAITING;
    SyncRepQueueInsert(mode);
    Assert(SyncRepQueueIsOrderedByLSN(mode));
    LWLockRelease(SyncRepLock);

    // Update process title to show waiting status
    if (update_process_title)
    {
        char buffer[32];
        sprintf(buffer, "waiting for %X/%X", LSN_FORMAT_ARGS(lsn));
        set_ps_display_suffix(buffer);
    }

    // Wait for replication confirmation
    for (;;)
    {
        int rc;
        ResetLatch(MyLatch);

        // Check if replication is complete
        if (MyProc->syncRepState == SYNC_REP_WAIT_COMPLETE)
            break;

        // Handle shutdown request
        if (ProcDiePending)
        {
            ereport(WARNING,
                    (errcode(ERRCODE_ADMIN_SHUTDOWN),
                     errmsg("canceling the wait for synchronous replication and terminating connection due to administrator command"),
                     errdetail("The transaction has already committed locally, but might not have been replicated to the standby.")));
            whereToSendOutput = DestNone;
            SyncRepCancelWait();
            break;
        }

        // Handle query cancel
        if (QueryCancelPending)
        {
            QueryCancelPending = false;
            ereport(WARNING,
                    (errmsg("canceling wait for synchronous replication due to user request"),
                     errdetail("The transaction has already committed locally, but might not have been replicated to the standby.")));
            SyncRepCancelWait();
            break;
        }

        // Wait for wakeup signal
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
                       WAIT_EVENT_SYNC_REP);

        // Handle postmaster death
        if (rc & WL_POSTMASTER_DEATH)
        {
            ProcDiePending = true;
            whereToSendOutput = DestNone;
            SyncRepCancelWait();
            break;
        }
    }

    // Clean up state after replication confirmed
    pg_read_barrier();
    Assert(dlist_node_is_detached(&MyProc->syncRepLinks));
    MyProc->syncRepState = SYNC_REP_NOT_WAITING;
    MyProc->waitLSN = 0;

    // Reset process title
    if (update_process_title)
        set_ps_display_remove_suffix();
}
```