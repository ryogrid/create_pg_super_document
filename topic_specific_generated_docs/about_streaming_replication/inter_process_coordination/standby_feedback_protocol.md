# Standby Feedback Protocol - Implementation Details

> **Related Documentation**: This implementation analysis extends the feedback coverage provided in:
> - **WalSender Feedback**: [Replication Sender Component - ProcessRepliesIfAny](../../../topic_specific_generated_docs/about_wal/component_replication_sender.md#processrepliesifany)
> - **WalReceiver Replies**: [Replication Receiver Component - Message Processing](../../../topic_specific_generated_docs/about_wal/component_replication_receiver.md#processing-flow)
>
> **Scope**: This section provides detailed message format specifications, protocol implementation mechanics, and performance optimization strategies not covered in the overview documentation above.

## Overview

This document provides detailed implementation analysis of the PostgreSQL standby feedback protocol that enables bidirectional communication between primary and standby servers during streaming replication. It focuses on write/flush/reply notification implementation, message format specifications, primary side handling mechanisms, and performance optimization strategies.

## Standby Side Feedback Generation

### 1. XLogWalRcvSendReply - Status Update Messages

**Function**: `XLogWalRcvSendReply(bool force, bool requestReply)`
**Location**: `src/backend/replication/walreceiver.c:1086-1156`

**Status Message Implementation**:
```c
static void XLogWalRcvSendReply(bool force, bool requestReply)
{
    static XLogRecPtr writePtr = 0;
    static XLogRecPtr flushPtr = 0;
    XLogRecPtr applyPtr;
    TimestampTz now;

    // Configuration-based early exit
    if (!force && wal_receiver_status_interval <= 0)
        return;

    now = GetCurrentTimestamp();

    // Optimization: avoid expensive spinlock operations for unchanged positions
    if (!force
        && writePtr == LogstreamResult.Write
        && flushPtr == LogstreamResult.Flush
        && now < wakeup[WALRCV_WAKEUP_REPLY])
        return;

    // Schedule next reply transmission
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_REPLY, now);

    // Capture current WAL positions
    writePtr = LogstreamResult.Write;    // Last WAL position written to disk
    flushPtr = LogstreamResult.Flush;    // Last WAL position flushed (durable)
    applyPtr = GetXLogReplayRecPtr(NULL); // Last WAL position applied during replay

    // Construct binary message with 'r' message type
    resetStringInfo(&reply_message);
    pq_sendbyte(&reply_message, 'r');              // Message type identifier
    pq_sendint64(&reply_message, writePtr);        // Write position (LSN)
    pq_sendint64(&reply_message, flushPtr);        // Flush position (LSN)
    pq_sendint64(&reply_message, applyPtr);        // Apply position (LSN)
    pq_sendint64(&reply_message, GetCurrentTimestamp()); // Current timestamp
    pq_sendbyte(&reply_message, requestReply ? 1 : 0);   // Reply request flag

    // Diagnostic logging
    elog(DEBUG2, "sending write %X/%X flush %X/%X apply %X/%X%s",
         LSN_FORMAT_ARGS(writePtr),
         LSN_FORMAT_ARGS(flushPtr),
         LSN_FORMAT_ARGS(applyPtr),
         requestReply ? " (reply requested)" : "");

    // Transmit message to primary
    walrcv_send(wrconn, reply_message.data, reply_message.len);
}
```

**Message Format Specification - 'r' (Reply) Message**:
```
Byte 0:      'r' (0x72) - Message type identifier
Bytes 1-8:   writePtr (uint64) - Last written WAL position
Bytes 9-16:  flushPtr (uint64) - Last flushed (durable) WAL position
Bytes 17-24: applyPtr (uint64) - Last applied (replayed) WAL position
Bytes 25-32: timestamp (uint64) - Current timestamp in microseconds since epoch
Byte 33:     requestReply (uint8) - 1 if reply requested, 0 otherwise
Total Length: 34 bytes
```

**Position Semantics and Guarantees**:
- **writePtr ≥ flushPtr ≥ applyPtr**: Strict ordering maintained
- **Write Position**: Data written to disk but not necessarily durable
- **Flush Position**: Data durably committed to storage (fsync completed)
- **Apply Position**: Data successfully applied during replay (visible to queries)

### 2. XLogWalRcvSendHSFeedback - Hot Standby Feedback

**Function**: `XLogWalRcvSendHSFeedback(bool immed)`
**Location**: `src/backend/replication/walreceiver.c:1158-1256`

**Hot Standby Feedback Implementation**:
```c
static void XLogWalRcvSendHSFeedback(bool immed)
{
    TimestampTz now;
    FullTransactionId nextFullXid;
    TransactionId nextXid;
    uint32 xmin_epoch, catalog_xmin_epoch;
    TransactionId xmin, catalog_xmin;

    static bool primary_has_standby_xmin = true;

    // Configuration and state checks
    if ((wal_receiver_status_interval <= 0 || !hot_standby_feedback) && !immed)
        return;

    // Skip if not in hot standby mode
    if (!HotStandbyActive())
        return;

    now = GetCurrentTimestamp();

    // Throttle feedback frequency unless immediate
    if (!immed && now < wakeup[WALRCV_WAKEUP_HSFEEDBACK])
        return;

    // Schedule next hot standby feedback
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_HSFEEDBACK, now);

    // Gather transaction visibility horizon information
    GetReplicationHorizons(&xmin, &catalog_xmin);

    // Get transaction ID epoch information
    GetNextFullTransactionId(&nextFullXid);
    nextXid = XidFromFullTransactionId(nextFullXid);
    xmin_epoch = EpochFromFullTransactionId(nextFullXid);
    catalog_xmin_epoch = xmin_epoch;

    // Handle epoch wraparound for older transactions
    if (TransactionIdPrecedes(xmin, nextXid)) {
        // xmin is from current epoch
    } else if (TransactionIdPrecedes(xmin, FirstNormalTransactionId)) {
        // xmin is a special XID (bootstrap, frozen)
        xmin_epoch = 0;
    } else {
        // xmin is from previous epoch
        xmin_epoch--;
    }

    // Same logic for catalog_xmin
    if (TransactionIdPrecedes(catalog_xmin, nextXid)) {
        // catalog_xmin is from current epoch
    } else if (TransactionIdPrecedes(catalog_xmin, FirstNormalTransactionId)) {
        // catalog_xmin is a special XID
        catalog_xmin_epoch = 0;
    } else {
        // catalog_xmin is from previous epoch
        catalog_xmin_epoch--;
    }

    elog(DEBUG2, "sending hot standby feedback xmin %u epoch %u catalog_xmin %u epoch %u",
         xmin, xmin_epoch, catalog_xmin, catalog_xmin_epoch);

    // Construct hot standby feedback message
    resetStringInfo(&reply_message);
    pq_sendbyte(&reply_message, 'h');                    // Message type identifier
    pq_sendint64(&reply_message, now);                   // Current timestamp
    pq_sendint32(&reply_message, xmin);                  // Oldest xmin from queries
    pq_sendint32(&reply_message, xmin_epoch);            // Epoch of xmin
    pq_sendint32(&reply_message, catalog_xmin);          // Oldest catalog xmin
    pq_sendint32(&reply_message, catalog_xmin_epoch);    // Epoch of catalog xmin

    // Transmit feedback to primary
    walrcv_send(wrconn, reply_message.data, reply_message.len);

    // Track whether primary supports standby xmin
    primary_has_standby_xmin = true;
}
```

**Message Format Specification - 'h' (Hot Standby Feedback) Message**:
```
Byte 0:      'h' (0x68) - Message type identifier
Bytes 1-8:   timestamp (uint64) - Current timestamp in microseconds since epoch
Bytes 9-12:  xmin (uint32) - Oldest transaction ID visible to standby queries
Bytes 13-16: xmin_epoch (uint32) - Epoch of xmin transaction ID
Bytes 17-20: catalog_xmin (uint32) - Oldest transaction ID for catalog visibility
Bytes 21-24: catalog_xmin_epoch (uint32) - Epoch of catalog_xmin transaction ID
Total Length: 25 bytes
```

**Hot Standby Feedback Purpose**:
- **Query Conflict Prevention**: Prevents primary from removing tuples still visible to standby queries
- **VACUUM Coordination**: Informs primary's VACUUM process about standby visibility requirements
- **Catalog Protection**: Separate tracking for system catalog vs user table visibility
- **Transaction Horizon Communication**: Communicates transaction visibility boundaries

### 3. Feedback Timing and Scheduling

#### WalRcvComputeNextWakeup - Timing Coordination
```c
static void WalRcvComputeNextWakeup(WalRcvWakeupReason reason, TimestampTz now)
{
    TimestampTz wakeup_time;

    switch (reason) {
        case WALRCV_WAKEUP_TERMINATE:
            // Timeout for connection termination
            if (wal_receiver_timeout > 0) {
                wakeup_time = TimestampTzPlusMilliseconds(now, wal_receiver_timeout);
            } else {
                wakeup_time = TIMESTAMP_TZ_MAX;  // No timeout
            }
            break;

        case WALRCV_WAKEUP_PING:
            // Keepalive ping interval
            if (wal_receiver_timeout > 0) {
                wakeup_time = TimestampTzPlusMilliseconds(now, wal_receiver_timeout / 2);
            } else {
                wakeup_time = TIMESTAMP_TZ_MAX;
            }
            break;

        case WALRCV_WAKEUP_REPLY:
            // Status reply interval
            if (wal_receiver_status_interval > 0) {
                wakeup_time = TimestampTzPlusMilliseconds(now, wal_receiver_status_interval * 1000);
            } else {
                wakeup_time = TIMESTAMP_TZ_MAX;
            }
            break;

        case WALRCV_WAKEUP_HSFEEDBACK:
            // Hot standby feedback interval
            if (hot_standby_feedback && wal_receiver_status_interval > 0) {
                wakeup_time = TimestampTzPlusMilliseconds(now, wal_receiver_status_interval * 1000);
            } else {
                wakeup_time = TIMESTAMP_TZ_MAX;
            }
            break;

        default:
            elog(ERROR, "unrecognized wakeup reason: %d", (int) reason);
    }

    // Store computed wakeup time
    wakeup[reason] = wakeup_time;
}
```

**Timing Strategy**:
- **Status Replies**: Sent every `wal_receiver_status_interval` seconds
- **Hot Standby Feedback**: Sent at same frequency as status replies when enabled
- **Keepalive Pings**: Sent at half the timeout interval to maintain connection
- **Connection Timeout**: Full timeout period before connection termination

## Primary Side Feedback Processing

### 4. ProcessStandbyReplyMessage - Reply Processing

**Function**: `ProcessStandbyReplyMessage(void)`
**Location**: `src/backend/replication/walsender.c:2378-2474`

**Reply Message Processing**:
```c
static void ProcessStandbyReplyMessage(void)
{
    XLogRecPtr writePtr, flushPtr, applyPtr;
    TimestampTz replyTime;
    bool replyRequested;
    TimeOffset writeLag, flushLag, applyLag;

    // Extract message fields
    writePtr = pq_getmsgint64(&reply_message);
    flushPtr = pq_getmsgint64(&reply_message);
    applyPtr = pq_getmsgint64(&reply_message);
    replyTime = pq_getmsgint64(&reply_message);
    replyRequested = pq_getmsgbyte(&reply_message);

    elog(DEBUG2, "write %X/%X flush %X/%X apply %X/%X%s",
         LSN_FORMAT_ARGS(writePtr),
         LSN_FORMAT_ARGS(flushPtr),
         LSN_FORMAT_ARGS(applyPtr),
         replyRequested ? " (reply requested)" : "");

    // Validate position ordering
    if (writePtr < flushPtr) {
        ereport(WARNING,
               (errmsg("standby reported flush position %X/%X higher than write position %X/%X",
                      LSN_FORMAT_ARGS(flushPtr),
                      LSN_FORMAT_ARGS(writePtr))));
        flushPtr = writePtr;  // Adjust to maintain consistency
    }

    if (flushPtr < applyPtr) {
        ereport(WARNING,
               (errmsg("standby reported apply position %X/%X higher than flush position %X/%X",
                      LSN_FORMAT_ARGS(applyPtr),
                      LSN_FORMAT_ARGS(flushPtr))));
        applyPtr = flushPtr;  // Adjust to maintain consistency
    }

    // Update walsender shared state
    SpinLockAcquire(&MyWalSnd->mutex);

    MyWalSnd->write = writePtr;
    MyWalSnd->flush = flushPtr;
    MyWalSnd->apply = applyPtr;
    MyWalSnd->replyTime = replyTime;

    // Calculate lag measurements
    writeLag = LagTrackerRead(LAG_TRACKER_WRITE, MyWalSnd);
    flushLag = LagTrackerRead(LAG_TRACKER_FLUSH, MyWalSnd);
    applyLag = LagTrackerRead(LAG_TRACKER_APPLY, MyWalSnd);

    MyWalSnd->writeLag = writeLag;
    MyWalSnd->flushLag = flushLag;
    MyWalSnd->applyLag = applyLag;

    SpinLockRelease(&MyWalSnd->mutex);

    // Wake up processes waiting for synchronous replication
    SyncRepReleaseWaiters();

    // Send reply if requested
    if (replyRequested) {
        WalSndKeepalive(false);
    }
}
```

**Processing Characteristics**:
- **Position Validation**: Ensures LSN ordering consistency (write ≥ flush ≥ apply)
- **Lag Calculation**: Updates lag tracking for monitoring and alerting
- **Synchronous Coordination**: Wakes up backends waiting for synchronous replication
- **Shared State Update**: Atomically updates walsender state under spinlock protection

### 5. ProcessStandbyHSFeedbackMessage - Hot Standby Feedback Processing

**Function**: `ProcessStandbyHSFeedbackMessage(void)`
**Location**: `src/backend/replication/walsender.c:2476-2554`

**Hot Standby Feedback Processing**:
```c
static void ProcessStandbyHSFeedbackMessage(void)
{
    TimestampTz replyTime;
    TransactionId xmin;
    uint32 xmin_epoch;
    TransactionId catalog_xmin;
    uint32 catalog_xmin_epoch;
    FullTransactionId xmin_full, catalog_xmin_full;

    // Extract hot standby feedback fields
    replyTime = pq_getmsgint64(&reply_message);
    xmin = pq_getmsgint32(&reply_message);
    xmin_epoch = pq_getmsgint32(&reply_message);
    catalog_xmin = pq_getmsgint32(&reply_message);
    catalog_xmin_epoch = pq_getmsgint32(&reply_message);

    elog(DEBUG2, "hot standby feedback xmin %u epoch %u catalog_xmin %u epoch %u",
         xmin, xmin_epoch, catalog_xmin, catalog_xmin_epoch);

    // Reconstruct full transaction IDs from epoch and XID
    xmin_full = FullTransactionIdFromEpochAndXid(xmin_epoch, xmin);
    catalog_xmin_full = FullTransactionIdFromEpochAndXid(catalog_xmin_epoch, catalog_xmin);

    // Update global standby feedback state
    SpinLockAcquire(&MyWalSnd->mutex);

    // Store transaction visibility horizons
    MyWalSnd->feedbackXmin = xmin_full;
    MyWalSnd->feedbackCatalogXmin = catalog_xmin_full;
    MyWalSnd->replyTime = replyTime;

    SpinLockRelease(&MyWalSnd->mutex);

    // Update global replication slot minimum LSN
    if (MyReplicationSlot && FullTransactionIdIsValid(xmin_full)) {
        PhysicalReplicationSlotNewXmin(xmin_full, catalog_xmin_full);
    }

    // Trigger vacuum process updates for global visibility
    SetTransactionVisibilityHorizon(xmin_full, catalog_xmin_full);
}
```

**Feedback Integration**:
- **Transaction ID Reconstruction**: Combines epoch and XID into full transaction IDs
- **Global State Update**: Updates shared walsender state with visibility horizons
- **Replication Slot Integration**: Updates slot state to prevent premature WAL removal
- **VACUUM Coordination**: Informs global transaction visibility calculations

### 6. Lag Tracking Implementation

#### LagTrackerWrite - Record Transmission Times
```c
void LagTrackerWrite(XLogRecPtr lsn, TimestampTz local_flush_time)
{
    TimestampTz now = GetCurrentTimestamp();
    LagTracker *tracker = &MyWalSnd->lag_tracker;

    // Record LSN and timestamp for lag calculation
    tracker->buffer[tracker->write_head].lsn = lsn;
    tracker->buffer[tracker->write_head].time = local_flush_time;

    // Advance write head (circular buffer)
    tracker->write_head = (tracker->write_head + 1) % LAG_TRACKER_BUFFER_SIZE;

    // Handle buffer overflow by advancing read head
    if (tracker->write_head == tracker->read_head) {
        tracker->read_head = (tracker->read_head + 1) % LAG_TRACKER_BUFFER_SIZE;
    }
}

// Read lag measurement for specific operation type
TimeOffset LagTrackerRead(int lag_type, WalSnd *walsnd)
{
    LagTracker *tracker = &walsnd->lag_tracker;
    XLogRecPtr target_lsn;
    TimestampTz now = GetCurrentTimestamp();
    int i;

    // Determine target LSN based on lag type
    switch (lag_type) {
        case LAG_TRACKER_WRITE:
            target_lsn = walsnd->write;
            break;
        case LAG_TRACKER_FLUSH:
            target_lsn = walsnd->flush;
            break;
        case LAG_TRACKER_APPLY:
            target_lsn = walsnd->apply;
            break;
        default:
            return -1;  // Invalid lag type
    }

    // Search for matching LSN in tracking buffer
    i = tracker->read_head;
    while (i != tracker->write_head) {
        if (tracker->buffer[i].lsn >= target_lsn) {
            // Found matching entry - calculate lag
            return TimestampDifferenceMilliseconds(tracker->buffer[i].time, now);
        }
        i = (i + 1) % LAG_TRACKER_BUFFER_SIZE;
    }

    return -1;  // No matching entry found
}
```

**Lag Tracking Characteristics**:
- **Circular Buffer**: Fixed-size buffer for efficient memory usage
- **Multi-Level Tracking**: Separate tracking for write, flush, and apply operations
- **Time Correlation**: Links LSN positions with transmission timestamps
- **Overflow Handling**: Gracefully handles buffer overflow by discarding old entries

### 7. Synchronous Replication Integration

#### SyncRepReleaseWaiters - Coordinate Synchronous Commits
```c
void SyncRepReleaseWaiters(void)
{
    volatile WalSndCtlData *walsndctl = WalSndCtl;
    int i;

    // Iterate through all walsenders to check synchronous replication state
    for (i = 0; i < max_wal_senders; i++) {
        volatile WalSnd *walsnd = &walsndctl->walsnds[i];
        XLogRecPtr write_pos, flush_pos, apply_pos;

        if (walsnd->pid == 0)
            continue;  // Inactive slot

        SpinLockAcquire(&walsnd->mutex);
        write_pos = walsnd->write;
        flush_pos = walsnd->flush;
        apply_pos = walsnd->apply;
        SpinLockRelease(&walsnd->mutex);

        // Check if this walsender satisfies any pending synchronous waits
        if (walsnd->sync_standby_priority > 0) {
            SyncRepCheckConfig();
            SyncRepWakeQueue(false, write_pos, flush_pos, apply_pos);
        }
    }
}

// Wake up backends waiting for synchronous replication confirmation
static void SyncRepWakeQueue(bool all, XLogRecPtr writePtr, XLogRecPtr flushPtr, XLogRecPtr applyPtr)
{
    PROC_QUEUE *wakeup = NULL;
    int mode;

    // Determine which backends can be awakened based on sync level
    for (mode = 0; mode < NUM_SYNC_REP_WAIT_MODE; mode++) {
        PROC_QUEUE *queue = &WalSndCtl->SyncRepQueue[mode];
        PGPROC *proc = queue->links.next;

        while (proc != (PGPROC *) queue) {
            PGPROC *next = proc->links.next;
            XLogRecPtr targetLSN = proc->waitLSN;

            // Check if standby has reached required LSN
            bool satisfied = false;
            switch (mode) {
                case SYNC_REP_WAIT_WRITE:
                    satisfied = writePtr >= targetLSN;
                    break;
                case SYNC_REP_WAIT_FLUSH:
                    satisfied = flushPtr >= targetLSN;
                    break;
                case SYNC_REP_WAIT_APPLY:
                    satisfied = applyPtr >= targetLSN;
                    break;
            }

            if (satisfied || all) {
                // Remove from wait queue and wake up
                SHMQueueDelete(&proc->links);
                SHMQueueInsertAfter(&wakeup->links, &proc->links);
                proc->syncRepState = SYNC_REP_WAIT_COMPLETE;
            }

            proc = next;
        }
    }

    // Wake up all satisfied waiters
    while (!SHMQueueEmpty(&wakeup->links)) {
        PGPROC *proc = (PGPROC *) SHMQueueNext(&wakeup->links,
                                              &wakeup->links,
                                              offsetof(PGPROC, links));
        SHMQueueDelete(&proc->links);
        SetLatch(&proc->procLatch);
    }
}
```

## Performance Characteristics

### 8. Protocol Efficiency Optimizations

#### Message Frequency Control
```c
// Adaptive feedback frequency based on activity level
static bool ShouldSendFeedback(TimestampTz now, bool force)
{
    static TimestampTz last_reply_time = 0;
    static XLogRecPtr last_write_pos = InvalidXLogRecPtr;
    static XLogRecPtr last_flush_pos = InvalidXLogRecPtr;

    // Always send if forced
    if (force)
        return true;

    // Throttle based on configuration
    if (wal_receiver_status_interval <= 0)
        return false;

    // Check if enough time has elapsed
    if (TimestampDifferenceMilliseconds(last_reply_time, now) <
        wal_receiver_status_interval * 1000)
        return false;

    // Check if positions have changed significantly
    XLogRecPtr current_write = LogstreamResult.Write;
    XLogRecPtr current_flush = LogstreamResult.Flush;

    if (current_write == last_write_pos && current_flush == last_flush_pos)
        return false;  // No progress to report

    // Update tracking variables
    last_reply_time = now;
    last_write_pos = current_write;
    last_flush_pos = current_flush;

    return true;
}
```

#### Network Optimization
- **Binary Protocol**: Compact binary message format reduces network overhead
- **Batched Replies**: Multiple status updates combined when possible
- **Compression**: Future enhancement could compress feedback for high-frequency scenarios
- **TCP Nagle Control**: Explicit control over TCP_NODELAY for latency vs throughput trade-offs

### 9. Monitoring and Diagnostics

#### Feedback Monitoring Views
```sql
-- Monitor replication feedback and lag
SELECT
    client_addr,
    application_name,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    write_lag,
    flush_lag,
    replay_lag,
    sync_state
FROM pg_stat_replication;

-- Hot standby feedback status
SELECT
    slot_name,
    active,
    xmin,
    catalog_xmin,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_type = 'physical';

-- Configuration parameters affecting feedback
SELECT name, setting, unit, context
FROM pg_settings
WHERE name IN ('wal_receiver_status_interval', 'hot_standby_feedback', 'wal_receiver_timeout');
```

#### Performance Diagnostics
```c
// Detailed feedback logging for troubleshooting
if (log_min_messages <= DEBUG2) {
    elog(DEBUG2, "feedback stats: messages_sent=%d avg_write_lag=%dms avg_flush_lag=%dms avg_apply_lag=%dms",
         feedback_message_count,
         average_write_lag_ms,
         average_flush_lag_ms,
         average_apply_lag_ms);
}
```

## Summary

The standby feedback protocol implementation provides:

1. **Bidirectional Communication**: Comprehensive status reporting from standby to primary
2. **Hot Standby Integration**: Query conflict prevention through transaction visibility feedback
3. **Lag Tracking**: Detailed performance monitoring and alerting capabilities
4. **Synchronous Coordination**: Efficient coordination for synchronous replication commits
5. **Protocol Efficiency**: Optimized message formats and transmission scheduling
6. **Error Recovery**: Robust handling of network issues and position inconsistencies
7. **Performance Monitoring**: Comprehensive metrics for operational visibility

This protocol ensures efficient and reliable communication between primary and standby servers, enabling features like synchronous replication, hot standby query processing, and comprehensive replication monitoring.