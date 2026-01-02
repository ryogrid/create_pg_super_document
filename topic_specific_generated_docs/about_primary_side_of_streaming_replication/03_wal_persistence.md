# Chapter 3: WAL Persistence

<- [Previous: WAL Generation](02_wal_generation_lsn.md) | [Index](index.md) | [Next: Walsender Transmission](04_walsender_transmission.md) ->

---

## Overview

This chapter covers how WAL data is written and flushed to disk, focusing on the `XLogFlush()` and `XLogWrite()` functions. WAL persistence is critical for transaction durability - a commit is not durable until the commit record is fsync'd to disk.

The key mechanisms covered include:
- Group commit optimization via `LWLockAcquireOrWait()`
- The WALWriteLock coordination
- Write/sync range determination
- Walsender wakeup after flush

**Related Diagrams:**
- [Figure 3: WAL Write/Sync Flow](diagrams/03_wal_write_sync_flow.mermaid) - XLogFlush and XLogWrite operation
- [Figure 4: WAL Buffer States](diagrams/04_wal_buffer_state.mermaid) - Buffer lifecycle transitions

---

## Processing Flow

The WAL persistence flow:

```
Backend commit
    |
    v
XLogFlush(commitLSN)
    |
    +---> Quick exit if already flushed
    |
    +---> WaitXLogInsertionsToFinish() -----> Wait for in-progress insertions
    |
    +---> LWLockAcquireOrWait(WALWriteLock) --> GROUP COMMIT KEY
    |         |
    |         +---> If not acquired: another did our work, loop back
    |         +---> If acquired: proceed to write
    |
    +---> CommitDelay optimization (optional)
    |
    +---> XLogWrite() -----> Write pages, fsync
    |         |
    |         +---> pg_pwrite() batch write
    |         +---> issue_xlog_fsync()
    |         +---> Update logWriteResult, logFlushResult
    |
    +---> WalSndWakeupProcessRequests() -----> Wake walsenders
```

---

## Implementation Details

### XLogFlush Function

**Location:** `src/backend/access/transam/xlog.c:2778`

**Signature:**
```c
void XLogFlush(XLogRecPtr record)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `record` | XLogRecPtr | LSN that must be flushed (typically commit record LSN) |

#### Step-by-Step Internal Logic

**Step 1: Recovery Check**
```c
// xlog.c:2792-2796
if (!XLogInsertAllowed())
{
    UpdateMinRecoveryPoint(record, false);
    return;
}
```
During recovery, we update minRecoveryPoint instead of flushing.

**Step 2: Quick Exit Check**
```c
// xlog.c:2798-2800
/* Quick exit if already known flushed */
if (record <= LogwrtResult.Flush)
    return;
```
Uses cached local copy of flush position. This fast path avoids locking when the target is already durable.

**Step 3: Group Commit Loop**

This is the core of the group commit mechanism:

```c
// xlog.c:2827-2907
for (;;)
{
    /* Check if already flushed */
    RefreshXLogWriteResult(LogwrtResult);
    if (record <= LogwrtResult.Flush)
        break;

    /* Get current write request */
    SpinLockAcquire(&XLogCtl->info_lck);
    if (WriteRqstPtr < XLogCtl->LogwrtRqst.Write)
        WriteRqstPtr = XLogCtl->LogwrtRqst.Write;
    SpinLockRelease(&XLogCtl->info_lck);

    /* Wait for in-progress insertions */
    insertpos = WaitXLogInsertionsToFinish(WriteRqstPtr);

    /* Try to get the write lock */
    if (!LWLockAcquireOrWait(WALWriteLock, LW_EXCLUSIVE))
    {
        /* Lock released while waiting - recheck if work done */
        continue;
    }

    /* Got the lock; verify still needed */
    RefreshXLogWriteResult(LogwrtResult);
    if (record <= LogwrtResult.Flush)
    {
        LWLockRelease(WALWriteLock);
        break;
    }

    /* CommitDelay optimization */
    if (CommitDelay > 0 && enableFsync &&
        MinimumActiveBackends(CommitSiblings))
    {
        pg_usleep(CommitDelay);
        insertpos = WaitXLogInsertionsToFinish(insertpos);
    }

    /* Perform the write and flush */
    WriteRqst.Write = insertpos;
    WriteRqst.Flush = insertpos;
    XLogWrite(WriteRqst, insertTLI, false);

    LWLockRelease(WALWriteLock);
    break;
}
```

**Step 4: Exit Critical Section and Wake Walsenders**
```c
// xlog.c:2909-2912
END_CRIT_SECTION();

/* wake up walsenders now that we've released heavily contended locks */
WalSndWakeupProcessRequests(true, !RecoveryInProgress());
```

**Cross-reference:** Walsender wakeup triggers [XLogSendPhysical()](04_walsender_transmission.md#xlogsendphysical-function) to transmit the newly flushed WAL.

---

### LWLockAcquireOrWait - Group Commit Key

The `LWLockAcquireOrWait()` function is essential for group commit:

```c
// Called from XLogFlush
if (!LWLockAcquireOrWait(WALWriteLock, LW_EXCLUSIVE))
{
    /* Lock was released while we waited, but we didn't acquire it.
     * Someone else may have done our flush for us. Loop back to check. */
    continue;
}
```

This pattern allows multiple backends to "piggyback" on a single fsync:

| Step | Backend A | Backend B | Backend C |
|------|-----------|-----------|-----------|
| 1 | Acquires WALWriteLock | - | - |
| 2 | Starts writing | Calls LWLockAcquireOrWait, waits | Calls LWLockAcquireOrWait, waits |
| 3 | Completes write/fsync | Still waiting | Still waiting |
| 4 | Releases lock | Wakes, returns false | Wakes, returns false |
| 5 | - | Checks LogwrtResult.Flush | Checks LogwrtResult.Flush |
| 6 | - | Already flushed, returns! | Already flushed, returns! |

**Result:** Three transactions committed with one fsync operation.

---

### XLogWrite Function

**Location:** `src/backend/access/transam/xlog.c:2296`

**Signature:**
```c
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `WriteRqst` | XLogwrtRqst | Contains Write and Flush target LSNs |
| `tli` | TimeLineID | Timeline to write to |
| `flexible` | bool | Can stop early if some work done (used by walwriter) |

#### Write Range Determination

The write range is determined by:

1. **Start position:** `LogwrtResult.Write` - where we left off last time
2. **End position:** `WriteRqst.Write` - typically `insertpos` from `WaitXLogInsertionsToFinish()`

```c
// Key logic in XLogWrite
while (LogwrtResult.Write < WriteRqst.Write)
{
    /* Verify we're not ahead of insert process */
    XLogRecPtr EndPtr = pg_atomic_read_u64(&XLogCtl->xlblocks[curridx]);
    if (LogwrtResult.Write >= EndPtr)
        elog(PANIC, "xlog write request is past end of log");

    /* Advance LogwrtResult.Write to end of current buffer page */
    LogwrtResult.Write = EndPtr;

    // ... page accumulation and write logic ...
}
```

#### Batch Writing

Multiple consecutive WAL pages are written in a single `pg_pwrite()` call:

```c
/* Perform the write */
char *from = XLogCtl->pages + startidx * (Size) XLOG_BLCKSZ;
Size nbytes = npages * (Size) XLOG_BLCKSZ;

pgstat_report_wait_start(WAIT_EVENT_WAL_WRITE);
written = pg_pwrite(openLogFile, from, nbytes, startoffset);
pgstat_report_wait_end();
```

**Optimization:** Batching reduces system call overhead significantly under high write rates.

#### Segment Completion Fsync

When a segment is completed, fsync immediately:

```c
if (finishing_seg)
{
    issue_xlog_fsync(openLogFile, openLogSegNo, tli);
    WalSndWakeupRequest();
    LogwrtResult.Flush = LogwrtResult.Write;

    if (XLogArchivingActive())
        XLogArchiveNotifySeg(openLogSegNo, tli);
}
```

**Rationale:**
- Avoid re-opening old segments later for fsync
- Allow archiver to process the completed segment immediately
- Wake walsenders to transmit the completed segment

#### Atomic Progress Updates

```c
/* Update shared-memory status */
pg_atomic_write_u64(&XLogCtl->logWriteResult, LogwrtResult.Write);
pg_write_barrier();
pg_atomic_write_u64(&XLogCtl->logFlushResult, LogwrtResult.Flush);
```

The barrier ensures readers see the correct ordering: `Flush <= Write`

**Cross-reference:** Walsenders use `GetFlushRecPtr()` to read `logFlushResult` atomically. See [Chapter 4](04_walsender_transmission.md#getflushrecptr-function).

---

### WaitXLogInsertionsToFinish Function

**Location:** `src/backend/access/transam/xlog.c:1505`

Ensures all in-progress insertions up to the target LSN are complete before writing.

```c
static XLogRecPtr
WaitXLogInsertionsToFinish(XLogRecPtr upto)
{
    XLogRecPtr finishedTo;

    for (int i = 0; i < NUM_XLOGINSERT_LOCKS; i++)
    {
        XLogRecPtr insertingAt;

        /* Check if this inserter is past our target */
        insertingAt = pg_atomic_read_u64(&WALInsertLocks[i].l.insertingAt);
        if (insertingAt != InvalidXLogRecPtr && insertingAt < upto)
        {
            /* Wait for this inserter */
            LWLockWaitForVar(&WALInsertLocks[i].l.lock,
                             &WALInsertLocks[i].l.insertingAt,
                             insertingAt);
        }
    }

    /* Return current insert position */
    finishedTo = pg_atomic_read_u64(&XLogCtl->logInsertResult);
    return finishedTo;
}
```

**Key mechanism:** Uses `LWLockWaitForVar()` to wait for specific inserters without acquiring their locks. This allows waiting only on inserters that haven't finished yet.

**Cross-reference:** See [Chapter 2](02_wal_generation_lsn.md#copyxlogrecordtowal-function) for how `insertingAt` is updated during record copy.

---

### WalSndWakeupProcessRequests Function

**Location:** `src/backend/replication/walsender.c:3707`

Wakes walsenders after WAL flush via condition variable broadcast:

```c
void
WalSndWakeup(bool physical, bool logical)
{
    if (physical)
        ConditionVariableBroadcast(&WalSndCtl->wal_flush_cv);

    if (logical)
        ConditionVariableBroadcast(&WalSndCtl->wal_replay_cv);
}
```

**Cross-reference:** Walsenders sleep on `wal_flush_cv` in [WalSndWait()](04_walsender_transmission.md#walsndwait-function).

---

## Diagrams

### Figure 3: WAL Write/Sync Flow

**Location:** [diagrams/03_wal_write_sync_flow.mermaid](diagrams/03_wal_write_sync_flow.mermaid)

This flowchart shows the complete XLogFlush and XLogWrite operation, including:
- Group commit loop with LWLockAcquireOrWait
- CommitDelay optimization
- Batch write and fsync operations
- Atomic progress updates

### Figure 4: WAL Buffer State Transitions

**Location:** [diagrams/04_wal_buffer_state.mermaid](diagrams/04_wal_buffer_state.mermaid)

This state diagram shows the lifecycle of WAL buffer pages:
- Uninitialized -> Initialized -> BeingWritten -> Completed -> PendingWrite -> Written -> Flushed -> Recyclable

---

## Configuration Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `wal_sync_method` | `fdatasync` | Fsync method: fsync, fdatasync, open_sync, open_datasync. `open_*` methods use O_SYNC/O_DSYNC flags. |
| `wal_writer_delay` | `200ms` | Background flush interval. Lower values reduce sync rep latency but increase I/O. |
| `wal_writer_flush_after` | `1MB` | Trigger flush after this much written. Prevents large I/O bursts. |
| `commit_delay` | `0` | Delay before flush for group commit. Non-zero values trade latency for throughput. |
| `commit_siblings` | `5` | Minimum active transactions to activate commit_delay. |
| `fsync` | `on` | Enable/disable fsync. **DANGEROUS if off** - risks data corruption. |

**Cross-reference:** See [Appendix C: Configuration Parameters](appendix_config_params.md) for complete documentation.

---

## Key Takeaways

1. **Group commit via LWLockAcquireOrWait:** Multiple backends can share a single fsync operation. When a backend releases WALWriteLock after flushing, waiting backends check if their work is done before reacquiring.

2. **Batch page writes:** Multiple consecutive WAL pages are batched into single `pg_pwrite()` calls, reducing system call overhead.

3. **Segment boundary fsync:** When a WAL segment is completed, it is immediately fsynced. This allows archiving and prevents reopening old segments.

4. **WalSndWakeupRequest:** Called after every fsync to wake walsenders. This ensures standbys receive WAL promptly.

5. **Atomic progress tracking:** `logWriteResult` and `logFlushResult` are updated atomically with proper barrier ordering. Readers see `Flush <= Write <= Insert`.

6. **WaitXLogInsertionsToFinish:** Ensures data is complete before writing. Uses `LWLockWaitForVar()` to wait on specific inserters without lock contention.

7. **WALWriteLock serialization:** All write/fsync operations are serialized by WALWriteLock. This prevents partial writes and ensures ordering.

---

## Related Sections

- **Previous:** [Chapter 2: WAL Generation](02_wal_generation_lsn.md) - How WAL records are created
- **Next:** [Chapter 4: Walsender Transmission](04_walsender_transmission.md) - How WAL is sent to standbys
- **Architecture:** [Chapter 1: Lock Hierarchy](01_architecture_overview.md#lock-hierarchy)

---

## Navigation

<- [Previous: WAL Generation](02_wal_generation_lsn.md) | [Index](index.md) | [Next: Walsender Transmission](04_walsender_transmission.md) ->
