# Chapter 7: Synchronous Replication Wait and Release

<- [Previous: Standby Response](06_standby_response.md) | [Index](index.md) | [Next: Client Response](08_client_response.md) ->

---

## Overview

This chapter covers the core synchronous replication mechanism - how backends wait for confirmation and how walsenders release them. This is the heart of synchronous replication, ensuring transaction durability across multiple servers.

The key functions are:
- `SyncRepWaitForLSN()`: Backend waits for confirmation
- `SyncRepReleaseWaiters()`: Walsender releases waiting backends
- `SyncRepWakeQueue()`: Wakes backends whose LSNs are satisfied

**Related Diagrams:**
- [Figure 9: Sync Wait/Release Sequence](diagrams/09_sync_wait_release_sequence.mermaid) - Backend wait and wakeup
- [Figure 10: SyncRepQueue States](diagrams/10_syncrep_queue_state.mermaid) - Queue state transitions

---

## Processing Flow

### Wait Path (Backend)

```
Backend after XLogFlush()
    |
    v
SyncRepWaitForLSN(commitLSN, true)
    |
    +---> Fast exit checks
    |         |
    |         +---> SyncRepRequested()? -----> Return if not configured
    |         +---> sync_standbys_status? -----> Return if no sync standbys
    |         +---> lsn <= WalSndCtl->lsn[mode]? -----> Return if already satisfied
    |
    +---> LWLockAcquire(SyncRepLock, LW_EXCLUSIVE)
    |
    +---> Insert into SyncRepQueue[mode]
    |         |
    |         +---> SyncRepQueueInsert(mode) -----> Maintain LSN order
    |
    +---> LWLockRelease(SyncRepLock)
    |
    +---> Wait loop
              |
              +---> WaitLatch(WAIT_EVENT_SYNC_REP)
              |
              +---> Check syncRepState == SYNC_REP_WAIT_COMPLETE
              |
              +---> Handle interrupts (ProcDiePending, QueryCancelPending)
```

### Release Path (Walsender)

```
After ProcessStandbyReplyMessage()
    |
    v
SyncRepReleaseWaiters()
    |
    +---> Quick exit checks
    |         |
    |         +---> sync_standby_priority == 0? -----> Return (not sync standby)
    |         +---> state != STREAMING/STOPPING? -----> Return
    |
    +---> LWLockAcquire(SyncRepLock, LW_EXCLUSIVE)
    |
    +---> SyncRepGetSyncRecPtr() -----> Calculate confirmed positions
    |         |
    |         +---> SyncRepGetCandidateStandbys()
    |         +---> Priority or Quorum calculation
    |
    +---> Update WalSndCtl->lsn[] for each mode
    |
    +---> SyncRepWakeQueue() for each mode
    |         |
    |         +---> Walk queue, remove satisfied entries
    |         +---> Set syncRepState = SYNC_REP_WAIT_COMPLETE
    |         +---> SetLatch() to wake backends
    |
    +---> LWLockRelease(SyncRepLock)
```

---

## Implementation Details

### SyncRepWaitForLSN Function

**Location:** `src/backend/replication/syncrep.c:147`

**Signature:**
```c
void SyncRepWaitForLSN(XLogRecPtr lsn, bool commit)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `lsn` | XLogRecPtr | LSN that must be confirmed by standbys |
| `commit` | bool | Whether this is a commit (affects mode selection) |

#### Source Code Analysis

```c
// syncrep.c:147-363
void
SyncRepWaitForLSN(XLogRecPtr lsn, bool commit)
{
    int         mode;

    /* Should be called with interrupts held */
    Assert(InterruptHoldoffCount > 0);

    /*
     * Fast exit if user has not requested sync replication, or there are no
     * sync replication standby names defined.
     */
    if (!SyncRepRequested() ||
        ((((volatile WalSndCtlData *) WalSndCtl)->sync_standbys_status) &
         (SYNC_STANDBY_INIT | SYNC_STANDBY_DEFINED)) == SYNC_STANDBY_INIT)
        return;

    /* Cap mode for non-commit records */
    if (commit)
        mode = SyncRepWaitMode;
    else
        mode = Min(SyncRepWaitMode, SYNC_REP_WAIT_FLUSH);

    Assert(dlist_node_is_detached(&MyProc->syncRepLinks));

    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
    Assert(MyProc->syncRepState == SYNC_REP_NOT_WAITING);

    /* Check if already satisfied */
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
        LWLockRelease(SyncRepLock);
        return;
    }

    /* Set up wait state */
    MyProc->waitLSN = lsn;
    MyProc->syncRepState = SYNC_REP_WAITING;
    SyncRepQueueInsert(mode);
    LWLockRelease(SyncRepLock);

    /* Alter ps display */
    if (update_process_title)
    {
        char buffer[32];
        sprintf(buffer, "waiting for %X/%X", LSN_FORMAT_ARGS(lsn));
        set_ps_display_suffix(buffer);
    }

    /* Wait loop */
    for (;;)
    {
        int rc;

        ResetLatch(MyLatch);

        /* Check if we're done (lock-free check is safe here) */
        if (MyProc->syncRepState == SYNC_REP_WAIT_COMPLETE)
            break;

        /* Handle interrupts */
        if (ProcDiePending)
        {
            ereport(WARNING, ...);
            whereToSendOutput = DestNone;
            SyncRepCancelWait();
            break;
        }

        if (QueryCancelPending)
        {
            QueryCancelPending = false;
            ereport(WARNING, ...);
            SyncRepCancelWait();
            break;
        }

        /* Wait on latch */
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
                       WAIT_EVENT_SYNC_REP);

        if (rc & WL_POSTMASTER_DEATH)
        {
            ProcDiePending = true;
            whereToSendOutput = DestNone;
            SyncRepCancelWait();
            break;
        }
    }

    /* Cleanup */
    pg_read_barrier();
    Assert(dlist_node_is_detached(&MyProc->syncRepLinks));
    MyProc->syncRepState = SYNC_REP_NOT_WAITING;
    MyProc->waitLSN = 0;
}
```

#### Key Design Points

**Fast Exit Paths:**

| Condition | Action |
|-----------|--------|
| `synchronous_commit = off` or `local` | Return immediately |
| `synchronous_standby_names` is empty | Return immediately |
| LSN already confirmed (`lsn <= WalSndCtl->lsn[mode]`) | Return immediately |

**Lock-Free State Check:**

```c
if (MyProc->syncRepState == SYNC_REP_WAIT_COMPLETE)
    break;
```

The latch ensures proper memory barrier - if state is `SYNC_REP_WAIT_COMPLETE`, the walsender has finished updating all necessary fields before setting the latch.

---

### SyncRepQueueInsert Function

**Location:** `src/backend/replication/syncrep.c:371`

Maintains LSN order in the queue for efficient release:

```c
static void
SyncRepQueueInsert(int mode)
{
    dlist_head *queue;
    dlist_iter iter;

    Assert(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);
    queue = &WalSndCtl->SyncRepQueue[mode];

    /* Insert in LSN order (usually at tail) */
    dlist_reverse_foreach(iter, queue)
    {
        PGPROC *proc = dlist_container(PGPROC, syncRepLinks, iter.cur);

        if (proc->waitLSN < MyProc->waitLSN)
        {
            dlist_insert_after(&proc->syncRepLinks, &MyProc->syncRepLinks);
            return;
        }
    }

    /* First entry or smallest LSN - insert at head */
    dlist_push_head(queue, &MyProc->syncRepLinks);
}
```

**Optimization:** Searching from tail is efficient because commits typically have increasing LSNs, so new entries usually go at the tail.

---

### SyncRepReleaseWaiters Function

**Location:** `src/backend/replication/syncrep.c:473`

**Signature:**
```c
void SyncRepReleaseWaiters(void)
```

#### Source Code Analysis

```c
// syncrep.c:473-573
void
SyncRepReleaseWaiters(void)
{
    volatile WalSndCtlData *walsndctl = WalSndCtl;
    XLogRecPtr  writePtr;
    XLogRecPtr  flushPtr;
    XLogRecPtr  applyPtr;
    bool        got_recptr;
    bool        am_sync;
    int         numwrite = 0;
    int         numflush = 0;
    int         numapply = 0;

    /* Quick exit if not a potential sync standby */
    if (MyWalSnd->sync_standby_priority == 0 ||
        (MyWalSnd->state != WALSNDSTATE_STREAMING &&
         MyWalSnd->state != WALSNDSTATE_STOPPING) ||
        XLogRecPtrIsInvalid(MyWalSnd->flush))
    {
        announce_next_takeover = true;
        return;
    }

    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

    /* Calculate synced positions among all sync standbys */
    got_recptr = SyncRepGetSyncRecPtr(&writePtr, &flushPtr, &applyPtr, &am_sync);

    /* Announce if we just became a sync standby */
    if (announce_next_takeover && am_sync)
    {
        announce_next_takeover = false;
        ereport(LOG, (errmsg("standby \"%s\" is now a synchronous standby...",
                             application_name)));
    }

    /* Exit if not enough sync standbys */
    if (!got_recptr || !am_sync)
    {
        LWLockRelease(SyncRepLock);
        announce_next_takeover = !am_sync;
        return;
    }

    /* Update confirmed LSNs and wake waiters */
    if (walsndctl->lsn[SYNC_REP_WAIT_WRITE] < writePtr)
    {
        walsndctl->lsn[SYNC_REP_WAIT_WRITE] = writePtr;
        numwrite = SyncRepWakeQueue(false, SYNC_REP_WAIT_WRITE);
    }
    if (walsndctl->lsn[SYNC_REP_WAIT_FLUSH] < flushPtr)
    {
        walsndctl->lsn[SYNC_REP_WAIT_FLUSH] = flushPtr;
        numflush = SyncRepWakeQueue(false, SYNC_REP_WAIT_FLUSH);
    }
    if (walsndctl->lsn[SYNC_REP_WAIT_APPLY] < applyPtr)
    {
        walsndctl->lsn[SYNC_REP_WAIT_APPLY] = applyPtr;
        numapply = SyncRepWakeQueue(false, SYNC_REP_WAIT_APPLY);
    }

    LWLockRelease(SyncRepLock);

    elog(DEBUG3, "released %d procs up to write %X/%X, ...",
         numwrite, LSN_FORMAT_ARGS(writePtr), ...);
}
```

#### Key Checks

| Check | Condition | Action |
|-------|-----------|--------|
| Priority | `sync_standby_priority == 0` | Return (not in synchronous_standby_names) |
| State | `state != STREAMING/STOPPING` | Return (not eligible to confirm) |
| Position | `flush == InvalidXLogRecPtr` | Return (no position to report) |

**Cross-reference:** See [Figure 5: Walsender State Machine](diagrams/05_walsender_state.mermaid) for state requirements.

---

### SyncRepGetSyncRecPtr Function

**Location:** `src/backend/replication/syncrep.c:585`

Calculates the confirmed positions across all sync standbys:

```c
static bool
SyncRepGetSyncRecPtr(XLogRecPtr *writePtr, XLogRecPtr *flushPtr,
                     XLogRecPtr *applyPtr, bool *am_sync)
{
    SyncRepStandbyData *sync_standbys;
    int num_standbys;

    /* Initialize defaults */
    *writePtr = InvalidXLogRecPtr;
    *flushPtr = InvalidXLogRecPtr;
    *applyPtr = InvalidXLogRecPtr;
    *am_sync = false;

    if (SyncRepConfig == NULL)
        return false;

    /* Get candidate sync standbys */
    num_standbys = SyncRepGetCandidateStandbys(&sync_standbys);

    /* Check if we're among the candidates */
    for (int i = 0; i < num_standbys; i++)
    {
        if (sync_standbys[i].is_me)
        {
            *am_sync = true;
            break;
        }
    }

    if (!(*am_sync) || num_standbys < SyncRepConfig->num_sync)
        return false;

    /* Calculate positions based on sync method */
    if (SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY)
    {
        /* Priority mode: oldest position of top N standbys */
        SyncRepGetOldestSyncRecPtr(writePtr, flushPtr, applyPtr,
                                   sync_standbys, num_standbys);
    }
    else
    {
        /* Quorum mode: Nth latest position */
        SyncRepGetNthLatestSyncRecPtr(writePtr, flushPtr, applyPtr,
                                      sync_standbys, num_standbys);
    }

    return true;
}
```

#### Priority vs Quorum Mode

| Mode | Configuration | Calculation |
|------|---------------|-------------|
| Priority | `FIRST N (...)` | Slowest position among top N standbys by priority |
| Quorum | `ANY N (...)` | Nth fastest position among all candidates |

---

### SyncRepWakeQueue Function

**Location:** `src/backend/replication/syncrep.c:906`

```c
// syncrep.c:906-954
static int
SyncRepWakeQueue(bool all, int mode)
{
    volatile WalSndCtlData *walsndctl = WalSndCtl;
    int         numprocs = 0;
    dlist_mutable_iter iter;

    Assert(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);
    Assert(LWLockHeldByMeInMode(SyncRepLock, LW_EXCLUSIVE));

    dlist_foreach_modify(iter, &WalSndCtl->SyncRepQueue[mode])
    {
        PGPROC *proc = dlist_container(PGPROC, syncRepLinks, iter.cur);

        /* Stop if LSN not yet satisfied (queue is ordered) */
        if (!all && walsndctl->lsn[mode] < proc->waitLSN)
            return numprocs;

        /* Remove from queue */
        dlist_delete_thoroughly(&proc->syncRepLinks);

        /* Memory barrier before state change */
        pg_write_barrier();

        /* Set state to complete */
        proc->syncRepState = SYNC_REP_WAIT_COMPLETE;

        /* Wake the backend */
        SetLatch(&(proc->procLatch));

        numprocs++;
    }

    return numprocs;
}
```

#### Memory Barrier Requirements

```c
/* Remove from queue first */
dlist_delete_thoroughly(&proc->syncRepLinks);

/* Barrier ensures queue removal is visible before state change */
pg_write_barrier();

/* Then change state */
proc->syncRepState = SYNC_REP_WAIT_COMPLETE;
```

This ordering is **critical**: The waiting backend checks state without holding the lock, so it must see the queue removal before the state change to avoid race conditions.

---

## Diagrams

### Figure 9: Sync Wait/Release Sequence

**Location:** [diagrams/09_sync_wait_release_sequence.mermaid](diagrams/09_sync_wait_release_sequence.mermaid)

Shows interaction between waiting backend and releasing walsender with proper synchronization points.

### Figure 10: SyncRepQueue States

**Location:** [diagrams/10_syncrep_queue_state.mermaid](diagrams/10_syncrep_queue_state.mermaid)

Shows state transitions:
- NOT_WAITING -> WAITING -> WAIT_COMPLETE -> NOT_WAITING
- Also shows CANCELLED path for interrupts

---

## Configuration Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `synchronous_commit` | `on` | Wait level: off/local/remote_write/on/remote_apply |
| `synchronous_standby_names` | `''` | List of sync standby names with method |

### synchronous_commit Levels

| Level | Wait For | SyncRepWaitMode |
|-------|----------|-----------------|
| `off` | Nothing (async) | N/A |
| `local` | Local flush | N/A |
| `remote_write` | Standby write | SYNC_REP_WAIT_WRITE (0) |
| `on` | Standby flush | SYNC_REP_WAIT_FLUSH (1) |
| `remote_apply` | Standby apply | SYNC_REP_WAIT_APPLY (2) |

### synchronous_standby_names Syntax

```
# Priority mode: first N matching standbys (ordered by priority)
synchronous_standby_names = 'FIRST 2 (standby1, standby2, standby3)'

# Quorum mode: any N matching standbys
synchronous_standby_names = 'ANY 2 (standby1, standby2, standby3)'
```

**Cross-reference:** See [Appendix C: Configuration Parameters](appendix_config_params.md) for complete documentation.

---

## Key Takeaways

1. **LSN-ordered queue insertion:** Waiting backends insert into the queue maintaining LSN order. This enables efficient batch release - once an LSN is satisfied, all earlier LSNs are too.

2. **Lock-free state check:** The backend's loop checks `syncRepState` without holding `SyncRepLock`. This is safe because the latch provides the necessary memory barrier.

3. **STREAMING/STOPPING requirement:** Only walsenders in STREAMING or STOPPING state can release waiters. CATCHUP walsenders are still catching up and cannot be trusted for sync rep.

4. **Priority vs Quorum modes:**
   - Priority: Uses slowest of top N standbys - ensures all N have the data
   - Quorum: Uses Nth fastest - only N need to have it

5. **Memory barrier ordering:** Queue removal must be visible before state change. The `pg_write_barrier()` ensures this ordering for the lock-free check on the backend side.

6. **Interrupt handling:** Interrupts (SIGTERM, query cancel) result in WARNING messages, not errors. The transaction is already committed locally - the wait is just for replication confirmation.

7. **WalSndCtl->lsn[] fast path:** The `lsn[]` array enables fast-path checks in `SyncRepWaitForLSN()` to avoid queue insertion for already-satisfied LSNs.

---

## Related Sections

- **Previous:** [Chapter 6: Standby Response](06_standby_response.md) - What triggers release
- **Next:** [Chapter 8: Client Response](08_client_response.md) - What happens after release
- **Architecture:** [Chapter 1: WalSndCtlData](01_architecture_overview.md#walsndctldata)

---

## Navigation

<- [Previous: Standby Response](06_standby_response.md) | [Index](index.md) | [Next: Client Response](08_client_response.md) ->
