# WalSender Transmission - Implementation Details

> **Related Documentation**: This implementation analysis extends the conceptual foundation provided in:
> - **Architectural Overview**: [Replication Sender Component - WalSndLoop](../../../topic_specific_generated_docs/about_wal/component_replication_sender.md#walsndloop)
> - **Data Structures**: [Replication Sender Component - WalSnd Structure](../../../topic_specific_generated_docs/about_wal/component_replication_sender.md#data-structures)
> - **Processing Flow**: [Replication Sender Component - Processing Flow](../../../topic_specific_generated_docs/about_wal/component_replication_sender.md#processing-flow)
>
> **Scope**: This section provides network transmission mechanics, buffer management specifics, and performance optimization details not covered in the overview documentation above.

## Overview

This document provides detailed implementation analysis of PostgreSQL's WalSender process, focusing on internal buffer management, network transmission mechanics, message protocol handling, and client connection management. The WalSender is responsible for streaming WAL data from primary to standby servers.

## WalSender Main Control Loop

### 1. WalSndLoop - Event-Driven Processing Model

**Function**: `WalSndLoop(WalSndSendDataCallback send_data)`
**Location**: `src/backend/replication/walsender.c:2784-2923`

**Core Event Loop Structure**:
```c
static void WalSndLoop(WalSndSendDataCallback send_data)
{
    TimestampTz last_flush = 0;

    last_reply_timestamp = GetCurrentTimestamp();
    waiting_for_ping_response = false;

    for (;;) {
        ResetLatch(MyLatch);           // Clear pending wakeups
        CHECK_FOR_INTERRUPTS();       // Handle signals

        // Process configuration changes
        if (ConfigReloadPending) {
            ProcessConfigFile(PGC_SIGHUP);
            SyncRepInitConfig();       // Update sync replication config
        }

        // Check for standby feedback
        ProcessRepliesIfAny();

        // Exit condition check
        if (streamingDoneReceiving && streamingDoneSending &&
            !pq_is_send_pending())
            break;

        // Core transmission logic
        if (!pq_is_send_pending())
            send_data();              // XLogSendPhysical or XLogSendLogical
        else
            WalSndCaughtUp = false;   // Have pending data

        // Flush network buffer
        if (pq_flush_if_writable() != 0)
            WalSndShutdown();

        // State transition management
        if (WalSndCaughtUp && !pq_is_send_pending()) {
            if (MyWalSnd->state == WALSNDSTATE_CATCHUP) {
                WalSndSetState(WALSNDSTATE_STREAMING);
            }
            if (got_SIGUSR2)
                WalSndDone(send_data);  // Shutdown handling
        }

        // Connection maintenance
        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary();

        // Event-driven waiting
        if ((WalSndCaughtUp && send_data != XLogSendLogical &&
             !streamingDoneSending) || pq_is_send_pending()) {

            long sleeptime = WalSndComputeSleeptime(GetCurrentTimestamp());
            int wakeEvents = !streamingDoneReceiving ? WL_SOCKET_READABLE : 0;

            if (pq_is_send_pending())
                wakeEvents |= WL_SOCKET_WRITEABLE;

            WalSndWait(wakeEvents, sleeptime, WAIT_EVENT_WAL_SENDER_MAIN);
        }
    }
}
```

**Performance Characteristics**:
- **Non-blocking I/O**: Uses `pq_flush_if_writable()` to avoid blocking on network writes
- **Event-driven Architecture**: `WalSndWait()` uses epoll/kqueue for efficient waiting
- **State Machine**: Transitions between CATCHUP and STREAMING states
- **Batching Optimization**: Sends data only when output buffer is not pending

**Key State Variables**:
- `WalSndCaughtUp`: Boolean indicating if sender is current with WAL generation
- `streamingDoneReceiving/streamingDoneSending`: Protocol termination flags
- `waiting_for_ping_response`: Keepalive coordination flag

### 2. Sleep Time Computation and Timing Control

**Function**: `WalSndComputeSleeptime(TimestampTz now)`
**Location**: `src/backend/replication/walsender.c:2715-2758`

**Implementation Details**:
```c
static long WalSndComputeSleeptime(TimestampTz now)
{
    long sleeptime = 10000;  // Default 10 seconds

    if (wal_sender_timeout > 0 && last_reply_timestamp > 0) {
        TimestampTz wakeup_time;
        long sec_to_timeout;
        long microsec_to_timeout;

        // Calculate when we need to wake up for timeout detection
        wakeup_time = TimestampTzPlusMilliseconds(last_reply_timestamp,
                                                  wal_sender_timeout);

        TimestampDifferenceMilliseconds(now, wakeup_time,
                                       &sec_to_timeout, &microsec_to_timeout);

        sleeptime = sec_to_timeout * 1000 + microsec_to_timeout / 1000;

        // Send keepalive at half timeout if waiting for ping response
        if (waiting_for_ping_response) {
            sleeptime = Min(sleeptime, wal_sender_timeout / 2);
        }
    }

    return sleeptime;
}
```

**Timeout Management Strategy**:
- **Base Sleep Time**: 10 seconds when no timeout configured
- **Half-Timeout Keepalive**: Sends keepalive at 50% of `wal_sender_timeout`
- **Dynamic Adjustment**: Sleep time decreases as timeout approaches
- **Ping Response Tracking**: Shorter sleep when waiting for standby response

## WAL Data Transmission

### 3. XLogSendPhysical - Core Physical Replication

**Function**: `XLogSendPhysical(void)`
**Location**: `src/backend/replication/walsender.c:3089-3404`

**Key Implementation Phases**:

#### Phase 1: Send Position Calculation
```c
static void XLogSendPhysical(void)
{
    XLogRecPtr SendRqstPtr;

    if (sendTimeLineIsHistoric) {
        // Historic timeline - send up to switch point
        SendRqstPtr = sendTimeLineValidUpto;
    } else if (am_cascading_walsender) {
        // Cascading standby - send replayed WAL
        SendRqstPtr = GetStandbyFlushRecPtr(&SendRqstTLI);

        // Check for promotion or timeline switch
        if (!RecoveryInProgress()) {
            am_cascading_walsender = false;
            becameHistoric = true;
        }
    } else {
        // Primary server - send flushed WAL only
        SendRqstPtr = GetFlushRecPtr(NULL);
    }

    // Record lag tracking information
    LagTrackerWrite(SendRqstPtr, GetCurrentTimestamp());
}
```

**Timeline Management**:
- **Historic Timeline Handling**: Automatically detects timeline switches
- **Cascading Replication**: Special logic for standby-to-standby streaming
- **Promotion Detection**: Handles standby promotion to primary
- **Safety Constraints**: Never sends unflushed WAL from primary

#### Phase 2: Transmission Unit Calculation
```c
// Calculate transmission boundaries
startptr = sentPtr;
endptr = startptr + MAX_SEND_SIZE;  // 16 * XLOG_BLCKSZ = 128KB

// Respect request boundaries
if (SendRqstPtr <= endptr) {
    endptr = SendRqstPtr;
    WalSndCaughtUp = (sendTimeLineIsHistoric) ? false : true;
} else {
    // Round down to page boundary - critical for record integrity
    endptr -= (endptr % XLOG_BLCKSZ);
    WalSndCaughtUp = false;
}

nbytes = endptr - startptr;
Assert(nbytes <= MAX_SEND_SIZE);
```

**Buffer Size Constraints**:
- **MAX_SEND_SIZE**: `#define MAX_SEND_SIZE (XLOG_BLCKSZ * 16)` = 128KB
- **Page Boundary Alignment**: Always round to 8KB boundaries to prevent record splits
- **Record Integrity**: WAL records never split across CopyData messages
- **Network Efficiency**: 128KB provides good balance between latency and throughput

#### Phase 3: Message Construction
```c
// Build CopyData message with WAL data
resetStringInfo(&output_message);
pq_sendbyte(&output_message, 'w');          // Message type identifier

pq_sendint64(&output_message, startptr);    // dataStart position
pq_sendint64(&output_message, SendRqstPtr); // walEnd position
pq_sendint64(&output_message, 0);           // sendtime (filled later)

// Pre-allocate buffer space to avoid reallocations
enlargeStringInfo(&output_message, nbytes);
```

**Message Format Specification**:
```
CopyData Message Format:
[1 byte] 'w' - WAL data message type
[8 bytes] dataStart - Starting LSN of this message
[8 bytes] walEnd - End LSN available on sender
[8 bytes] sendtime - Timestamp when message was sent
[N bytes] WAL data - Actual WAL record data
```

#### Phase 4: Optimized Data Reading
```c
retry:
// Attempt WAL buffer read first (fastest path)
rbytes = WALReadFromBuffers(&output_message.data[output_message.len],
                           startptr, nbytes, xlogreader->seg.ws_tli);
output_message.len += rbytes;
startptr += rbytes;
nbytes -= rbytes;

// Fall back to disk read for remaining data
if (nbytes > 0 &&
    !WALRead(xlogreader,
             &output_message.data[output_message.len],
             startptr, nbytes, xlogreader->seg.ws_tli, &errinfo))
    WALReadRaiseError(&errinfo);

// Handle cascading standby file reloads
if (am_cascading_walsender) {
    SpinLockAcquire(&walsnd->mutex);
    reload = walsnd->needreload;
    walsnd->needreload = false;
    SpinLockRelease(&walsnd->mutex);

    if (reload && xlogreader->seg.ws_file >= 0) {
        wal_segment_close(xlogreader);
        goto retry;  // Reopen and retry read
    }
}
```

**Read Optimization Strategy**:
1. **WAL Buffer Priority**: `WALReadFromBuffers()` for recently generated WAL
2. **Disk Fallback**: `WALRead()` for older WAL data
3. **Zero-Copy Design**: Read directly into output message buffer
4. **File Reload Handling**: Cascading standbys handle WAL file replacement

#### Phase 5: Network Transmission
```c
// Fill timestamp just before sending
resetStringInfo(&tmpbuf);
pq_sendint64(&tmpbuf, GetCurrentTimestamp());
memcpy(&output_message.data[1 + sizeof(int64) + sizeof(int64)],
       tmpbuf.data, sizeof(int64));

// Non-blocking send to network
pq_putmessage_noblock('d', output_message.data, output_message.len);

// Update sent position atomically
sentPtr = endptr;
SpinLockAcquire(&walsnd->mutex);
walsnd->sentPtr = sentPtr;
SpinLockRelease(&walsnd->mutex);
```

**Network Protocol Details**:
- **Non-blocking Transmission**: `pq_putmessage_noblock()` queues data without blocking
- **Late Timestamp**: Timestamp filled just before transmission for accuracy
- **Atomic Updates**: Sent position updated under spinlock protection
- **Copy Protocol**: Uses PostgreSQL's COPY protocol for data streaming

### 4. Network Buffer Management

#### Internal Buffer Architecture
```c
// PostgreSQL libpq output buffer management
static StringInfoData output_message;  // Per-connection output buffer
static StringInfoData tmpbuf;          // Temporary formatting buffer

// Buffer state tracking
bool pq_is_send_pending(void);         // Check for unsent data
int pq_flush_if_writable(void);        // Non-blocking flush attempt
```

**Buffer Management Strategy**:
- **StringInfo Buffers**: Dynamic string buffers with automatic expansion
- **Single Message Assembly**: One complete message constructed at a time
- **Memory Reuse**: Buffers reset via `resetStringInfo()` between messages
- **Overflow Protection**: `enlargeStringInfo()` pre-allocates space

#### Flow Control and Back-pressure
```c
// In WalSndLoop main processing
if (!pq_is_send_pending())
    send_data();                    // Only send new data if buffer empty
else
    WalSndCaughtUp = false;        // Mark as behind due to network congestion

// Network write readiness
if (pq_flush_if_writable() != 0)
    WalSndShutdown();              // Handle network errors

// Wait for writeable socket when buffer full
if (pq_is_send_pending())
    wakeEvents |= WL_SOCKET_WRITEABLE;
```

**Back-pressure Handling**:
- **Send Throttling**: No new WAL data sent until buffer drains
- **Socket Monitoring**: Waits for writeable socket events
- **Congestion Detection**: Sets `WalSndCaughtUp = false` when network blocks
- **Error Propagation**: Network failures trigger clean shutdown

## Standby Feedback Processing

### 5. ProcessRepliesIfAny - Message Reception

**Function**: `ProcessRepliesIfAny(void)`
**Location**: `src/backend/replication/walsender.c:2220-2332`

**Non-blocking Message Processing**:
```c
static void ProcessRepliesIfAny(void)
{
    unsigned char firstchar;
    int maxmsglen;
    int r;
    bool received = false;

    last_processing = GetCurrentTimestamp();

    while (!streamingDoneReceiving) {
        pq_startmsgread();
        r = pq_getbyte_if_available(&firstchar);

        if (r < 0) {
            // Connection error - terminate
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                   errmsg("unexpected EOF on standby connection")));
            proc_exit(0);
        }
        if (r == 0) {
            // No data available - return immediately
            pq_endmsgread();
            break;
        }

        // Message type validation and size limits
        switch (firstchar) {
            case PqMsg_CopyData:
                maxmsglen = PQ_LARGE_MESSAGE_LIMIT;  // Standby status messages
                break;
            case PqMsg_CopyDone:
            case PqMsg_Terminate:
                maxmsglen = PQ_SMALL_MESSAGE_LIMIT;  // Control messages
                break;
            default:
                ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("invalid standby message type \"%c\"", firstchar)));
        }

        // Read complete message
        resetStringInfo(&reply_message);
        if (pq_getmessage(&reply_message, maxmsglen)) {
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                   errmsg("unexpected EOF on standby connection")));
            proc_exit(0);
        }

        // Dispatch message processing
        switch (firstchar) {
            case PqMsg_CopyData:
                ProcessStandbyMessage();       // Handle 'r' and 'h' messages
                received = true;
                break;
            case PqMsg_CopyDone:
                if (!streamingDoneSending) {
                    pq_putmessage_noblock('c', NULL, 0);  // Echo CopyDone
                    streamingDoneSending = true;
                }
                streamingDoneReceiving = true;
                received = true;
                break;
            case PqMsg_Terminate:
                proc_exit(0);               // Clean disconnect
        }
    }

    // Update reply timestamp for timeout calculation
    if (received) {
        last_reply_timestamp = last_processing;
        waiting_for_ping_response = false;
    }
}
```

**Message Processing Characteristics**:
- **Non-blocking Reads**: `pq_getbyte_if_available()` returns immediately if no data
- **Protocol Validation**: Strict message type checking with fatal errors
- **Size Limits**: Different limits for data vs control messages
- **Connection State**: Tracks protocol termination states
- **Timeout Reset**: Updates reply timestamp for keepalive calculations

### 6. ProcessStandbyMessage - Message Dispatch

**Function**: `ProcessStandbyMessage(void)`
**Location**: `src/backend/replication/walsender.c:2338-2368`

**Message Type Dispatcher**:
```c
static void ProcessStandbyMessage(void)
{
    char msgtype;

    msgtype = pq_getmsgbyte(&reply_message);

    switch (msgtype) {
        case 'r':  // Standby reply message
            ProcessStandbyReplyMessage();
            break;
        case 'h':  // Hot standby feedback message
            ProcessStandbyHSFeedbackMessage();
            break;
        default:
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                   errmsg("unexpected standby message type \"%c\"", msgtype)));
            proc_exit(0);
    }
}
```

**Standby Message Types**:
1. **'r' Messages**: Standard reply with write/flush/apply positions
2. **'h' Messages**: Hot standby feedback with transaction visibility info
3. **Error Handling**: Protocol violations cause immediate termination

## Connection Management and Error Recovery

### 7. Connection Lifecycle Management

#### Connection State Tracking
```c
// Global state variables in walsender.c
static bool streamingDoneReceiving = false;  // Received CopyDone from standby
static bool streamingDoneSending = false;    // Sent CopyDone to standby
static bool waiting_for_ping_response = false;  // Keepalive state
static TimestampTz last_reply_timestamp = 0;    // Last message from standby
```

#### Keepalive and Timeout Management
```c
// WalSndKeepaliveIfNecessary implementation
static void WalSndKeepaliveIfNecessary(void)
{
    TimestampTz now = GetCurrentTimestamp();

    if (wal_sender_timeout > 0 &&
        TimestampDifferenceExceeds(last_reply_timestamp, now,
                                  wal_sender_timeout / 2) &&
        !waiting_for_ping_response) {

        // Send keepalive message
        pq_putmessage_noblock('k', keepalive_message, keepalive_len);
        waiting_for_ping_response = true;
    }
}

// WalSndCheckTimeOut implementation
static void WalSndCheckTimeOut(void)
{
    TimestampTz now = GetCurrentTimestamp();

    if (wal_sender_timeout > 0 &&
        TimestampDifferenceExceeds(last_reply_timestamp, now, wal_sender_timeout)) {

        ereport(COMMERROR, (errmsg("terminating walsender due to replication timeout")));
        WalSndShutdown();
    }
}
```

**Keepalive Protocol**:
- **Half-Timeout Trigger**: Keepalive sent at 50% of timeout period
- **Response Tracking**: `waiting_for_ping_response` prevents duplicate keepalives
- **Timeout Detection**: Full timeout triggers connection termination
- **Non-blocking Send**: Keepalives use `pq_putmessage_noblock()`

### 8. Error Handling and Recovery Mechanisms

#### Network Error Handling
```c
// Network write error detection
if (pq_flush_if_writable() != 0) {
    WalSndShutdown();           // Clean shutdown on network error
}

// Connection loss detection
if (r < 0) {  // From pq_getbyte_if_available()
    ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
           errmsg("unexpected EOF on standby connection")));
    proc_exit(0);
}
```

#### WAL Read Error Handling
```c
// In XLogSendPhysical
if (!WALRead(xlogreader, buffer, startptr, nbytes, tli, &errinfo)) {
    WALReadRaiseError(&errinfo);  // Convert to appropriate error level
}

// Cascading standby file reload handling
if (am_cascading_walsender && reload_needed) {
    wal_segment_close(xlogreader);
    goto retry;  // Attempt file reload and retry
}
```

#### Timeline Management Errors
```c
// Timeline switch detection in cascading scenario
if (sendTimeLine != SendRqstTLI) {
    // Read timeline history and find switch point
    history = readTimeLineHistory(SendRqstTLI);
    sendTimeLineValidUpto = tliSwitchPoint(sendTimeLine, history, &sendTimeLineNextTLI);
    sendTimeLineIsHistoric = true;
}
```

## Performance Characteristics and Optimization

### 9. Critical Performance Paths

#### Latency Optimizations
1. **WAL Buffer Priority**: `WALReadFromBuffers()` eliminates disk I/O for recent WAL
2. **Non-blocking I/O**: All network operations avoid blocking
3. **Event-driven Architecture**: `WalSndWait()` uses efficient system calls
4. **Late Timestamping**: Message timestamps filled just before transmission

#### Throughput Optimizations
1. **Batching**: 128KB messages reduce protocol overhead
2. **Zero-copy Reads**: Direct read into output buffer
3. **Buffer Reuse**: StringInfo buffers reused across messages
4. **Page Alignment**: Prevents WAL record fragmentation

#### Memory Management
1. **Dynamic Buffers**: StringInfo automatically expands as needed
2. **Pre-allocation**: `enlargeStringInfo()` reserves space before reads
3. **Buffer Reset**: `resetStringInfo()` reuses existing memory
4. **Shared Memory**: WalSnd struct coordinates with other processes

### 10. Configuration Parameters Impact

#### Network and Timing Parameters
- **wal_sender_timeout**: Controls keepalive frequency and connection timeout
- **tcp_keepalives_***: OS-level TCP keepalive settings
- **max_wal_senders**: Affects shared memory allocation

#### Buffer and Performance Parameters
- **wal_buffers**: Affects `WALReadFromBuffers()` hit rate
- **wal_writer_delay**: Controls WAL flush frequency affecting send rate
- **synchronous_commit**: Determines when WalSender acknowledgment required

#### Protocol Parameters
- **MAX_SEND_SIZE**: Hardcoded 128KB transmission unit
- **XLOG_BLCKSZ**: 8KB page size affects alignment requirements
- **PQ_LARGE_MESSAGE_LIMIT**: Maximum standby message size

## Debugging and Monitoring

### Key Monitoring Queries
```sql
-- WalSender status and lag
SELECT application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
       write_lag, flush_lag, replay_lag
FROM pg_stat_replication;

-- Network buffer status
SELECT pid, backend_type, wait_event_type, wait_event
FROM pg_stat_activity
WHERE backend_type = 'walsender';

-- WalSender configuration
SELECT name, setting, unit, context
FROM pg_settings
WHERE name LIKE 'wal_sender%' OR name LIKE 'max_wal_senders';
```

### Performance Diagnostics
- **Lag Analysis**: Monitor lag columns in pg_stat_replication
- **Wait Events**: Check for WalSenderWait events indicating network congestion
- **Buffer Hit Rate**: Analyze WAL buffer effectiveness via wal_buffers configuration

## Summary

The WalSender process implements a sophisticated event-driven architecture optimized for streaming replication performance:

1. **Event-Driven Loop**: Non-blocking I/O with efficient event waiting
2. **Optimized Reading**: WAL buffers prioritized over disk reads
3. **Protocol Efficiency**: 128KB messages with careful boundary alignment
4. **Back-pressure Handling**: Flow control prevents memory exhaustion
5. **Robust Error Handling**: Comprehensive connection and data error recovery
6. **Timeline Management**: Handles primary promotion and timeline switches
7. **Performance Monitoring**: Lag tracking and connection status reporting

The implementation balances latency requirements for synchronous replication with throughput needs for high-volume asynchronous replication, providing a robust foundation for PostgreSQL's streaming replication capabilities.