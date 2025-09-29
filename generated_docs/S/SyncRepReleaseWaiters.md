# SyncRepReleaseWaiters

## Location
[src/backend/replication/syncrep.c:474-585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L474-L585)

## Overview
Updates synchronous replication LSN positions and releases waiting backend processes based on the current state of synchronous standbys.

## Definition

```c
void
SyncRepReleaseWaiters(void)
```
## Detailed Description
This function implements PostgreSQL's synchronous replication policy by updating LSN positions on replication queues and releasing waiting backend processes. It follows a "first-valid-sync-standby-releases-waiter" policy where the first synchronous standby that confirms receipt allows waiting transactions to proceed.

The function performs several key operations:
1. Validates that the current WAL sender is serving a potential sync standby
2. Acquires exclusive lock on synchronous replication state
3. Determines current sync positions using 
4. Updates global LSN positions for write, flush, and apply operations
5. Wakes up waiting backend processes that can now proceed
6. Logs takeover announcements when a standby becomes synchronous

The function exits early if the WAL sender is not eligible (priority 0, invalid state, or invalid flush position).

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- Uses  to check current WAL sender state and priority
- Modifies  array to update global LSN positions
- References  for synchronous replication method configuration

## Dependencies
- Functions called/Symbols referenced:
  -  - Gets current sync LSN positions
  -  - Wakes waiting processes for each sync level
  -  - Validates LSN positions
  - / - Manages SyncRepLock
  -  - Logs sync standby announcements
- Called from:
  -  (src/backend/replication/walsender.c:2495)

## Notes and Other Information
- Uses SyncRepLock for thread-safe access to shared synchronous replication state
- Supports both priority-based and quorum-based synchronous replication methods
- The  flag prevents redundant logging of sync standby status
- Debug logging at level 3 shows detailed information about released processes and LSN positions
- Function location: src/backend/replication/syncrep.c:474-585

## Simplified Source

```c
void SyncRepReleaseWaiters(void) {
    volatile WalSndCtlData *walsndctl = WalSndCtl;
    XLogRecPtr writePtr, flushPtr, applyPtr;
    bool got_recptr, am_sync;
    int numwrite = 0, numflush = 0, numapply = 0;

    // Early exit if this WAL sender is not a sync standby candidate
    if (MyWalSnd->sync_standby_priority == 0 ||
        (MyWalSnd->state != WALSNDSTATE_STREAMING &&
         MyWalSnd->state != WALSNDSTATE_STOPPING) ||
        XLogRecPtrIsInvalid(MyWalSnd->flush)) {
        announce_next_takeover = true;
        return;
    }

    // Get exclusive lock on sync replication state
    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

    // Get current sync positions among all sync standbys
    got_recptr = SyncRepGetSyncRecPtr(&writePtr, &flushPtr, &applyPtr, &am_sync);

    // Announce if we just became a sync standby
    if (announce_next_takeover && am_sync) {
        announce_next_takeover = false;
        if (SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY)
            ereport(LOG, (errmsg("standby \"%s\" is now a synchronous standby with priority %d",
                                application_name, MyWalSnd->sync_standby_priority)));
        else
            ereport(LOG, (errmsg("standby \"%s\" is now a candidate for quorum synchronous standby",
                                application_name)));
    }

    // Exit if insufficient sync standbys or we're not managing a sync standby
    if (!got_recptr || !am_sync) {
        LWLockRelease(SyncRepLock);
        announce_next_takeover = !am_sync;
        return;
    }

    // Update LSN positions and wake waiters
    if (walsndctl->lsn[SYNC_REP_WAIT_WRITE] < writePtr) {
        walsndctl->lsn[SYNC_REP_WAIT_WRITE] = writePtr;
        numwrite = SyncRepWakeQueue(false, SYNC_REP_WAIT_WRITE);
    }
    if (walsndctl->lsn[SYNC_REP_WAIT_FLUSH] < flushPtr) {
        walsndctl->lsn[SYNC_REP_WAIT_FLUSH] = flushPtr;
        numflush = SyncRepWakeQueue(false, SYNC_REP_WAIT_FLUSH);
    }
    if (walsndctl->lsn[SYNC_REP_WAIT_APPLY] < applyPtr) {
        walsndctl->lsn[SYNC_REP_WAIT_APPLY] = applyPtr;
        numapply = SyncRepWakeQueue(false, SYNC_REP_WAIT_APPLY);
    }

    LWLockRelease(SyncRepLock);

    elog(DEBUG3, "released %d procs up to write %X/%X, %d procs up to flush %X/%X, %d procs up to apply %X/%X",
         numwrite, LSN_FORMAT_ARGS(writePtr),
         numflush, LSN_FORMAT_ARGS(flushPtr),
         numapply, LSN_FORMAT_ARGS(applyPtr));
}
```