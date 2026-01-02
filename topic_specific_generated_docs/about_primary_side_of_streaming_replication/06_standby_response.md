# Chapter 6: Standby Response Processing

<- [Previous: Keepalive Monitoring](05_keepalive_monitoring.md) | [Index](index.md) | [Next: Sync Wait/Release](07_sync_wait_release.md) ->

---

## Overview

This chapter covers how the walsender processes replies from the standby server. The standby periodically sends position updates (write, flush, apply LSNs) which the walsender uses to:

- Track replication lag
- Update shared memory state
- Release waiting backends (synchronous replication)
- Advance replication slot positions

The key function is `ProcessStandbyReplyMessage()`.

**Related Diagrams:**
- [Figure 8: Standby Response Sequence](diagrams/08_standby_response_sequence.mermaid) - Reply processing sequence

---

## Processing Flow

The standby response processing flow:

```
Socket input available
    |
    v
ProcessRepliesIfAny()
    |
    +---> pq_getbyte_if_available() -----> Get message type
    |
    +---> ProcessStandbyMessage() -----> Dispatch by type
              |
              +---> 'r': ProcessStandbyReplyMessage()
              |         |
              |         +---> Parse write/flush/apply LSNs
              |         +---> Calculate lag via LagTrackerRead()
              |         +---> Update WalSnd shared memory
              |         +---> SyncRepReleaseWaiters() -----> CRITICAL
              |         +---> Advance replication slot
              |
              +---> 'h': ProcessStandbyHSFeedbackMessage()
                          |
                          +---> Update xmin for vacuum
```

---

## Implementation Details

### ProcessStandbyReplyMessage Function

**Location:** `src/backend/replication/walsender.c:2405`

**Signature:**
```c
static void ProcessStandbyReplyMessage(void)
```

#### Source Code Analysis

```c
// walsender.c:2405-2507
static void
ProcessStandbyReplyMessage(void)
{
    XLogRecPtr  writePtr,
                flushPtr,
                applyPtr;
    bool        replyRequested;
    TimeOffset  writeLag,
                flushLag,
                applyLag;
    bool        clearLagTimes;
    TimestampTz now;
    TimestampTz replyTime;

    static bool fullyAppliedLastTime = false;

    /* Parse the reply message (caller consumed msgtype byte) */
    writePtr = pq_getmsgint64(&reply_message);
    flushPtr = pq_getmsgint64(&reply_message);
    applyPtr = pq_getmsgint64(&reply_message);
    replyTime = pq_getmsgint64(&reply_message);
    replyRequested = pq_getmsgbyte(&reply_message);

    if (message_level_is_interesting(DEBUG2))
    {
        elog(DEBUG2, "write %X/%X flush %X/%X apply %X/%X%s reply_time %s",
             LSN_FORMAT_ARGS(writePtr),
             LSN_FORMAT_ARGS(flushPtr),
             LSN_FORMAT_ARGS(applyPtr),
             replyRequested ? " (reply requested)" : "",
             timestamptz_to_str(replyTime));
    }

    /* Calculate lag times using LagTracker */
    now = GetCurrentTimestamp();
    writeLag = LagTrackerRead(SYNC_REP_WAIT_WRITE, writePtr, now);
    flushLag = LagTrackerRead(SYNC_REP_WAIT_FLUSH, flushPtr, now);
    applyLag = LagTrackerRead(SYNC_REP_WAIT_APPLY, applyPtr, now);

    /* Clear stale lag data if standby is fully caught up */
    clearLagTimes = false;
    if (applyPtr == sentPtr)
    {
        if (fullyAppliedLastTime)
            clearLagTimes = true;
        fullyAppliedLastTime = true;
    }
    else
        fullyAppliedLastTime = false;

    /* Send reply if standby requested one */
    if (replyRequested)
        WalSndKeepalive(false, InvalidXLogRecPtr);

    /* Update shared memory state */
    {
        WalSnd *walsnd = MyWalSnd;

        SpinLockAcquire(&walsnd->mutex);
        walsnd->write = writePtr;
        walsnd->flush = flushPtr;
        walsnd->apply = applyPtr;
        if (writeLag != -1 || clearLagTimes)
            walsnd->writeLag = writeLag;
        if (flushLag != -1 || clearLagTimes)
            walsnd->flushLag = flushLag;
        if (applyLag != -1 || clearLagTimes)
            walsnd->applyLag = applyLag;
        walsnd->replyTime = replyTime;
        SpinLockRelease(&walsnd->mutex);
    }

    /* Release waiting backends (sync rep) */
    if (!am_cascading_walsender)
        SyncRepReleaseWaiters();

    /* Advance replication slot if configured */
    if (MyReplicationSlot && flushPtr != InvalidXLogRecPtr)
    {
        if (SlotIsLogical(MyReplicationSlot))
            LogicalConfirmReceivedLocation(flushPtr);
        else
            PhysicalConfirmReceivedLocation(flushPtr);
    }
}
```

#### Step-by-Step Analysis

**Step 1: Parse Reply Message**

```c
writePtr = pq_getmsgint64(&reply_message);
flushPtr = pq_getmsgint64(&reply_message);
applyPtr = pq_getmsgint64(&reply_message);
replyTime = pq_getmsgint64(&reply_message);
replyRequested = pq_getmsgbyte(&reply_message);
```

The message contains three position indicators:

| Position | Meaning | Used For |
|----------|---------|----------|
| `writePtr` | WAL written to standby's disk (not necessarily fsynced) | `synchronous_commit = remote_write` |
| `flushPtr` | WAL fsynced on standby | `synchronous_commit = on` (default) |
| `applyPtr` | WAL applied/replayed by startup process | `synchronous_commit = remote_apply` |

**Cross-reference:** See [Appendix B: Glossary](appendix_glossary.md#synchronous_commit-levels) for synchronization level details.

**Step 2: Calculate Lag**

```c
writeLag = LagTrackerRead(SYNC_REP_WAIT_WRITE, writePtr, now);
flushLag = LagTrackerRead(SYNC_REP_WAIT_FLUSH, flushPtr, now);
applyLag = LagTrackerRead(SYNC_REP_WAIT_APPLY, applyPtr, now);
```

`LagTrackerRead()` calculates the time difference between when the walsender sent the WAL (recorded in [XLogSendPhysical()](04_walsender_transmission.md#xlogsendphysical-function)) and when the standby confirmed it.

**Step 3: Update Shared Memory**

```c
SpinLockAcquire(&walsnd->mutex);
walsnd->write = writePtr;
walsnd->flush = flushPtr;
walsnd->apply = applyPtr;
// ... lag values ...
walsnd->replyTime = replyTime;
SpinLockRelease(&walsnd->mutex);
```

These values are visible via `pg_stat_replication` system view.

**Step 4: Release Sync Rep Waiters**

```c
if (!am_cascading_walsender)
    SyncRepReleaseWaiters();
```

**Critical:** Only primary walsenders release waiters. Cascading walsenders don't participate in sync rep because they are not authoritative about the primary's data durability.

**Cross-reference:** See [Chapter 7](07_sync_wait_release.md#syncrepreleasewaiters-function) for detailed release logic.

**Step 5: Advance Replication Slot**

```c
if (MyReplicationSlot && flushPtr != InvalidXLogRecPtr)
{
    PhysicalConfirmReceivedLocation(flushPtr);
}
```

The slot's `restart_lsn` is advanced based on confirmed flush position. This prevents WAL cleanup of segments the standby still needs.

---

### ProcessRepliesIfAny Function

**Location:** `src/backend/replication/walsender.c:2224`

Non-blocking check for incoming messages:

```c
static void
ProcessRepliesIfAny(void)
{
    unsigned char firstchar;
    int         maxmsglen;
    int         r;
    bool        received = false;

    for (;;)
    {
        pq_startmsgread();
        r = pq_getbyte_if_available(&firstchar);

        if (r < 0)
        {
            /* Connection error */
            ereport(COMMERROR, ...);
            proc_exit(0);
        }
        if (r == 0)
        {
            /* No data available */
            pq_endmsgread();
            break;
        }

        /* Got a message, process it */
        switch (firstchar)
        {
            case 'd':
                /* CopyData containing standby message */
                if (pq_getmessage(&reply_message, 0))
                    proc_exit(0);
                ProcessStandbyMessage();
                received = true;
                break;

            case 'c':
                /* CopyDone - client finished */
                streamingDoneReceiving = true;
                pq_putmessage_noblock('c', NULL, 0);
                streamingDoneSending = true;
                break;

            case 'X':
                /* Terminate */
                proc_exit(0);
                break;
        }
    }

    if (received)
    {
        last_reply_timestamp = GetCurrentTimestamp();
        waiting_for_ping_response = false;
    }
}
```

**Key point:** `last_reply_timestamp` is updated after receiving any message, resetting the timeout counter. See [Chapter 5](05_keepalive_monitoring.md).

---

### ProcessStandbyMessage Function

**Location:** `src/backend/replication/walsender.c:2337`

Dispatches by message type:

```c
static void
ProcessStandbyMessage(void)
{
    char msgtype;

    msgtype = pq_getmsgbyte(&reply_message);

    switch (msgtype)
    {
        case 'r':
            ProcessStandbyReplyMessage();
            break;

        case 'h':
            ProcessStandbyHSFeedbackMessage();
            break;

        default:
            ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                     errmsg("unexpected message type \"%c\"", msgtype)));
            proc_exit(0);
    }
}
```

| Message Type | Function | Purpose |
|--------------|----------|---------|
| 'r' | `ProcessStandbyReplyMessage()` | Position and lag update |
| 'h' | `ProcessStandbyHSFeedbackMessage()` | Hot standby feedback for vacuum |

---

### LagTrackerWrite / LagTrackerRead

**Location:** `src/backend/replication/walsender.c:3772` / `3813`

The lag tracker records timestamps when WAL is sent and calculates lag when confirmed:

```c
// In XLogSendPhysical()
LagTrackerWrite(SendRqstPtr, GetCurrentTimestamp());

// LagTrackerWrite stores (LSN, timestamp) pairs in a circular buffer
static void
LagTrackerWrite(XLogRecPtr lsn, TimestampTz now)
{
    lag_tracker[lag_tracker_write_head].lsn = lsn;
    lag_tracker[lag_tracker_write_head].time = now;
    lag_tracker_write_head = (lag_tracker_write_head + 1) % LAG_TRACKER_BUFFER_SIZE;
}

// LagTrackerRead finds the timestamp when given LSN was sent
static TimeOffset
LagTrackerRead(int mode, XLogRecPtr lsn, TimestampTz now)
{
    // Search for LSN in lag_tracker[]
    // Return (now - send_time) as the lag
}
```

The calculated lag values appear in `pg_stat_replication.write_lag`, `flush_lag`, and `apply_lag`.

---

## Diagrams

### Figure 8: Standby Response Sequence

**Location:** [diagrams/08_standby_response_sequence.mermaid](diagrams/08_standby_response_sequence.mermaid)

This diagram shows:
- Message parsing (write/flush/apply LSNs)
- Shared memory updates under spinlock
- Transition to SyncRepReleaseWaiters
- Replication slot advancement

---

## Configuration Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `wal_receiver_status_interval` | 10s | How often standby sends status replies. Lower values improve sync rep responsiveness but increase network traffic. |
| `hot_standby_feedback` | off | Whether standby sends xmin feedback to prevent vacuum conflicts. |

**Cross-reference:** See [Appendix C: Configuration Parameters](appendix_config_params.md) for complete documentation.

---

## Key Takeaways

1. **Three positions tracked:** The standby reports write, flush, and apply positions separately, allowing different `synchronous_commit` levels.

2. **Lag calculation:** Uses stored timestamps from send time. The `LagTracker` circular buffer maintains send times for recently sent LSNs.

3. **Spinlock-protected update:** Shared memory update happens under `WalSnd.mutex` spinlock. The hold time is minimal (just field assignments).

4. **SyncRepReleaseWaiters called after every reply:** On primary walsenders, this is called after processing each reply to promptly release waiting backends.

5. **Cascading walsenders excluded:** `am_cascading_walsender` check prevents cascading standbys from participating in sync rep - only direct connections to the primary matter.

6. **Replication slot advancement:** Slot's `restart_lsn` advances based on flush position, preventing WAL cleanup of needed segments.

7. **Reply resets timeout:** Processing any message from standby updates `last_reply_timestamp`, resetting the timeout counter.

---

## Related Sections

- **Previous:** [Chapter 5: Keepalive Monitoring](05_keepalive_monitoring.md) - What triggers replies
- **Next:** [Chapter 7: Sync Wait/Release](07_sync_wait_release.md) - How waiters are released
- **Walsender Loop:** [Chapter 4: WalSndLoop](04_walsender_transmission.md#walsndloop-function) - Overall context

---

## Navigation

<- [Previous: Keepalive Monitoring](05_keepalive_monitoring.md) | [Index](index.md) | [Next: Sync Wait/Release](07_sync_wait_release.md) ->
