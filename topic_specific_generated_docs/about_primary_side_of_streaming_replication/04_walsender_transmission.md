# Chapter 4: Walsender Transmission

<- [Previous: WAL Persistence](03_wal_persistence.md) | [Index](index.md) | [Next: Keepalive Monitoring](05_keepalive_monitoring.md) ->

---

## Overview

This chapter covers how the walsender process transmits WAL data to standby servers. The walsender is a critical component of streaming replication, responsible for:

- Reading WAL from buffers or files
- Packaging WAL into CopyData messages
- Managing the replication protocol
- Tracking transmission progress

The key functions analyzed are `WalSndLoop()` and `XLogSendPhysical()`.

**Related Diagrams:**
- [Figure 5: Walsender State Machine](diagrams/05_walsender_state.mermaid) - WalSndState transitions
- [Figure 6: Send Data Structure](diagrams/06_send_data_structure.mermaid) - CopyData message format
- [Figure 7: Walsender Iteration](diagrams/07_walsender_iteration.mermaid) - Single WalSndLoop iteration

---

## Processing Flow

The walsender transmission flow:

```
WalSndLoop() main event loop
    |
    +---> ProcessRepliesIfAny() -----> Handle standby messages
    |
    +---> XLogSendPhysical() -----> Send WAL data
    |         |
    |         +---> GetFlushRecPtr() -----> Determine safe send position
    |         +---> WALReadFromBuffers() -----> Lock-free buffer read
    |         +---> WALRead() -----> Fallback to file read
    |         +---> pq_putmessage_noblock() -----> Queue for sending
    |
    +---> pq_flush_if_writable() -----> Flush socket buffer
    |
    +---> WalSndCheckTimeOut() -----> Timeout detection
    |
    +---> WalSndKeepaliveIfNecessary() -----> Send keepalive
    |
    +---> WalSndWait() -----> Wait for events
```

---

## Implementation Details

### WalSndLoop Function

**Location:** `src/backend/replication/walsender.c:2785`

**Signature:**
```c
static void WalSndLoop(WalSndSendDataCallback send_data)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `send_data` | WalSndSendDataCallback | Function pointer: `XLogSendPhysical` or `XLogSendLogical` |

#### Main Loop Operations

```c
// walsender.c:2785-2923
static void
WalSndLoop(WalSndSendDataCallback send_data)
{
    /* Initialize timeout tracking */
    last_reply_timestamp = GetCurrentTimestamp();
    waiting_for_ping_response = false;

    for (;;)
    {
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

        /* Handle configuration reload */
        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            SyncRepInitConfig();
        }

        /* Check for input from the client */
        ProcessRepliesIfAny();

        /* Check if streaming is complete */
        if (streamingDoneReceiving && streamingDoneSending &&
            !pq_is_send_pending())
            break;

        /* Send more WAL if output buffer is empty */
        if (!pq_is_send_pending())
            send_data();  /* XLogSendPhysical() */
        else
            WalSndCaughtUp = false;

        /* Try to flush pending output */
        if (pq_flush_if_writable() != 0)
            WalSndShutdown();

        /* State transition when caught up */
        if (WalSndCaughtUp && !pq_is_send_pending())
        {
            if (MyWalSnd->state == WALSNDSTATE_CATCHUP)
                WalSndSetState(WALSNDSTATE_STREAMING);

            if (got_SIGUSR2)
                WalSndDone(send_data);
        }

        /* Check for timeout and send keepalive if needed */
        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary();

        /* Wait for events */
        if ((WalSndCaughtUp && !streamingDoneSending) || pq_is_send_pending())
        {
            long sleeptime = WalSndComputeSleeptime(now);
            int wakeEvents = WL_SOCKET_READABLE;

            if (pq_is_send_pending())
                wakeEvents |= WL_SOCKET_WRITEABLE;

            WalSndWait(wakeEvents, sleeptime, WAIT_EVENT_WAL_SENDER_MAIN);
        }
    }
}
```

#### Key Loop Operations

| Operation | Purpose | Cross-reference |
|-----------|---------|-----------------|
| `ProcessRepliesIfAny()` | Handle standby messages | [Chapter 6](06_standby_response.md) |
| `send_data()` | Transmit WAL data | [XLogSendPhysical](#xlogsendphysical-function) |
| `pq_flush_if_writable()` | Flush socket buffer | - |
| `WalSndCheckTimeOut()` | Check wal_sender_timeout | [Chapter 5](05_keepalive_monitoring.md) |
| `WalSndKeepaliveIfNecessary()` | Send keepalive if needed | [Chapter 5](05_keepalive_monitoring.md) |
| `WalSndWait()` | Sleep until wakeup event | [WalSndWait](#walsndwait-function) |

---

### XLogSendPhysical Function

**Location:** `src/backend/replication/walsender.c:3099`

This function determines what WAL can be safely sent and transmits it.

**Signature:**
```c
static void XLogSendPhysical(void)
```

#### Source Code Analysis

```c
// walsender.c:3099-3404
static void
XLogSendPhysical(void)
{
    XLogRecPtr  SendRqstPtr;
    XLogRecPtr  startptr;
    XLogRecPtr  endptr;
    Size        nbytes;

    /* Handle stopping state */
    if (got_STOPPING)
        WalSndSetState(WALSNDSTATE_STOPPING);

    if (streamingDoneSending)
    {
        WalSndCaughtUp = true;
        return;
    }

    /* Determine safe send position */
    if (sendTimeLineIsHistoric)
    {
        /* Streaming old timeline */
        SendRqstPtr = sendTimeLineValidUpto;
    }
    else if (am_cascading_walsender)
    {
        /* Cascading standby - use replay position */
        SendRqstPtr = GetStandbyFlushRecPtr(&SendRqstTLI);
    }
    else
    {
        /* Primary - use flush position */
        SendRqstPtr = GetFlushRecPtr(NULL);
    }

    /* Record for lag tracking */
    LagTrackerWrite(SendRqstPtr, GetCurrentTimestamp());

    /* Check if caught up */
    if (SendRqstPtr <= sentPtr)
    {
        WalSndCaughtUp = true;
        return;
    }

    /* Calculate send range */
    startptr = sentPtr;
    endptr = startptr + MAX_SEND_SIZE;

    if (SendRqstPtr <= endptr)
    {
        endptr = SendRqstPtr;
        WalSndCaughtUp = true;
    }
    else
    {
        /* Round down to page boundary */
        endptr -= (endptr % XLOG_BLCKSZ);
        WalSndCaughtUp = false;
    }

    nbytes = endptr - startptr;
    Assert(nbytes <= MAX_SEND_SIZE);

    /* Build CopyData message */
    resetStringInfo(&output_message);
    pq_sendbyte(&output_message, 'w');           /* WAL data message */
    pq_sendint64(&output_message, startptr);     /* dataStart */
    pq_sendint64(&output_message, SendRqstPtr);  /* walEnd */
    pq_sendint64(&output_message, 0);            /* sendtime (filled later) */

    enlargeStringInfo(&output_message, nbytes);

    /* Read WAL data - first try buffers (faster, no locks) */
    rbytes = WALReadFromBuffers(&output_message.data[output_message.len],
                                startptr, nbytes, xlogreader->seg.ws_tli);
    output_message.len += rbytes;
    startptr += rbytes;
    nbytes -= rbytes;

    /* Read remaining from files */
    if (nbytes > 0)
    {
        if (!WALRead(xlogreader, &output_message.data[output_message.len],
                     startptr, nbytes, xlogreader->seg.ws_tli, &errinfo))
            WALReadRaiseError(&errinfo);
    }

    /* Send the message */
    pq_putmessage_noblock('d', output_message.data, output_message.len);

    /* Update progress */
    sentPtr = endptr;

    /* Update shared memory */
    SpinLockAcquire(&MyWalSnd->mutex);
    MyWalSnd->sentPtr = sentPtr;
    SpinLockRelease(&MyWalSnd->mutex);
}
```

#### Flow Control: GetFlushRecPtr

The walsender only sends WAL that has been **flushed** (durable on primary):

```c
/* Primary - use flush position */
SendRqstPtr = GetFlushRecPtr(NULL);
```

This ensures standbys never receive WAL that could be lost if the primary crashes.

**Cross-reference:** `GetFlushRecPtr()` reads `XLogCtl->logFlushResult` updated by [XLogWrite()](03_wal_persistence.md#xlogwrite-function).

#### Message Format

The WAL data message ('w') format:

| Field | Size | Description |
|-------|------|-------------|
| msgtype | 1 byte | 'w' for WAL data |
| dataStart | 8 bytes | Start position of WAL in this message |
| walEnd | 8 bytes | Current flush position on primary |
| sendTime | 8 bytes | Server timestamp |
| data | variable | WAL data bytes (up to MAX_SEND_SIZE) |

#### MAX_SEND_SIZE and Boundaries

```c
#define MAX_SEND_SIZE (g_libpq_buffer_size - 4)
```

With default libpq buffer size of 16MB, MAX_SEND_SIZE is approximately 16MB - 4 bytes.

**Boundary handling:**
- If more than MAX_SEND_SIZE to send, round down to page boundary
- WAL records can span pages but never span messages
- Page boundary is safe because continuation records start at page beginning

---

### WALReadFromBuffers Function

**Location:** `src/backend/access/transam/xlog.c:1749`

Lock-free read from WAL buffers using atomic verification:

```c
Size
WALReadFromBuffers(char *dstbuf, XLogRecPtr startptr, Size count,
                   TimeLineID tli)
{
    char *page;
    int idx;
    XLogRecPtr expectedEndPtr;
    XLogRecPtr endptr;

    /* Read is within a single page */
    idx = XLogRecPtrToBufIdx(startptr);
    page = XLogCtl->pages + idx * (Size) XLOG_BLCKSZ;

    /* Verify buffer contains expected data - first check */
    expectedEndPtr = startptr - (startptr % XLOG_BLCKSZ) + XLOG_BLCKSZ;
    endptr = pg_atomic_read_u64(&XLogCtl->xlblocks[idx]);

    if (expectedEndPtr != endptr)
        return 0;  /* Buffer doesn't contain our data */

    /* Read the data */
    memcpy(dstbuf, page + (startptr % XLOG_BLCKSZ), count);

    pg_read_barrier();

    /* Verify buffer wasn't recycled during read - second check */
    endptr = pg_atomic_read_u64(&XLogCtl->xlblocks[idx]);
    if (expectedEndPtr != endptr)
        return 0;  /* Buffer was recycled, data invalid */

    return count;
}
```

**Double-check pattern:** This ensures data validity without locks:
1. Check `xlblocks[idx]` before read
2. Read data with `memcpy`
3. Memory barrier
4. Check `xlblocks[idx]` again after read

If the buffer was recycled between checks, the second check detects it and falls back to file read.

---

### GetFlushRecPtr Function

**Location:** `src/backend/access/transam/xlog.c:6445`

Returns current WAL flush position:

```c
XLogRecPtr
GetFlushRecPtr(TimeLineID *insertTLI)
{
    Assert(XLogCtl != NULL);

    if (insertTLI)
        *insertTLI = XLogCtl->InsertTimeLineID;

    return pg_atomic_read_u64(&XLogCtl->logFlushResult);
}
```

This is the primary flow control mechanism - walsender only sends WAL that has been flushed.

---

### WalSndWait Function

**Location:** `src/backend/replication/walsender.c:3728`

```c
static void
WalSndWait(uint32 socket_events, long timeout, uint32 wait_event)
{
    WaitEvent event;

    ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, socket_events, NULL);

    /* Prepare to sleep on condition variable for efficient wakeup */
    if (MyWalSnd->kind == REPLICATION_KIND_PHYSICAL)
        ConditionVariablePrepareToSleep(&WalSndCtl->wal_flush_cv);
    else if (MyWalSnd->kind == REPLICATION_KIND_LOGICAL)
        ConditionVariablePrepareToSleep(&WalSndCtl->wal_replay_cv);

    /* Wait for socket or CV wakeup */
    if (WaitEventSetWait(FeBeWaitSet, timeout, &event, 1, wait_event) == 1 &&
        (event.events & WL_POSTMASTER_DEATH))
    {
        ConditionVariableCancelSleep();
        proc_exit(1);
    }

    ConditionVariableCancelSleep();
}
```

**Key mechanism:** Uses condition variables (`wal_flush_cv`) for efficient wakeup from [WalSndWakeupProcessRequests()](03_wal_persistence.md#walsndwakeupprocessrequests-function) after XLogFlush.

---

## Diagrams

### Figure 5: Walsender State Machine

**Location:** [diagrams/05_walsender_state.mermaid](diagrams/05_walsender_state.mermaid)

Shows WalSndState transitions:
- STARTUP -> CATCHUP -> STREAMING -> STOPPING
- Only STREAMING and STOPPING states can participate in sync rep

### Figure 6: Transmission Data Structure

**Location:** [diagrams/06_send_data_structure.mermaid](diagrams/06_send_data_structure.mermaid)

Shows CopyData message format and MAX_SEND_SIZE boundaries.

### Figure 7: Walsender Single Iteration

**Location:** [diagrams/07_walsender_iteration.mermaid](diagrams/07_walsender_iteration.mermaid)

Shows one complete iteration of WalSndLoop with all operations.

---

## Configuration Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `max_wal_senders` | 10 | Maximum concurrent walsender processes |
| `wal_sender_timeout` | 60s | Timeout for standby response |
| `wal_keep_size` | 0 | Minimum WAL to retain for standbys |
| `max_slot_wal_keep_size` | -1 | Maximum WAL retained by slots |

**Cross-reference:** See [Appendix C: Configuration Parameters](appendix_config_params.md) for complete documentation.

---

## Key Takeaways

1. **Flow control via GetFlushRecPtr:** Walsender only sends WAL that has been flushed (`GetFlushRecPtr()`). This ensures standbys never receive WAL that could be lost.

2. **MAX_SEND_SIZE limits:** Message size is limited to approximately 16MB. Larger amounts are split at page boundaries.

3. **Lock-free buffer reading:** `WALReadFromBuffers()` reads from WAL buffers without locks using atomic double-check pattern. Falls back to file read if buffer was recycled.

4. **Page boundaries are safe:** WAL is never split mid-record across messages. Page boundaries are safe cut points because continuation records start at page beginning.

5. **Condition variable wakeup:** Walsenders sleep on `wal_flush_cv` and are woken by `XLogFlush()`, eliminating polling overhead.

6. **State transitions:** CATCHUP -> STREAMING transition controls sync rep participation. Only STREAMING or STOPPING walsenders can release sync rep waiters.

7. **sentPtr tracking:** The `sentPtr` variable tracks what has been queued for sending. Updated in shared memory (`MyWalSnd->sentPtr`) for monitoring visibility.

---

## Related Sections

- **Previous:** [Chapter 3: WAL Persistence](03_wal_persistence.md) - How WAL is flushed before sending
- **Next:** [Chapter 5: Keepalive Monitoring](05_keepalive_monitoring.md) - Timeout and keepalive handling
- **Reply Processing:** [Chapter 6: Standby Response](06_standby_response.md) - How replies are processed

---

## Navigation

<- [Previous: WAL Persistence](03_wal_persistence.md) | [Index](index.md) | [Next: Keepalive Monitoring](05_keepalive_monitoring.md) ->
