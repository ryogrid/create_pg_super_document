# XLogSendPhysical

## Location
[src/backend/replication/walsender.c:3100-3409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3100-L3409)

## Overview
XLogSendPhysical reads and streams physical WAL data to replication clients, handling timeline switches, buffering constraints, and various replication scenarios including primary-standby and cascading replication.

## Definition

```c
static void
XLogSendPhysical(void)
```
## Detailed Description
XLogSendPhysical is the core function responsible for streaming physical WAL data to replication clients. It implements sophisticated logic to handle multiple replication scenarios:

1. **Historic Timeline Streaming**: When streaming from a historic timeline, it ensures streaming only up to the timeline switch point.

2. **Cascading Replication**: On standby servers acting as cascading WAL senders, it streams WAL that has been replayed or received, detecting timeline changes and promotions dynamically.

3. **Primary Streaming**: On primary servers, it streams all WAL that has been flushed to disk for durability guarantees.

The function implements intelligent buffering by reading up to MAX_SEND_SIZE bytes, with careful boundary alignment to ensure WAL records are never split across messages. It attempts to read from WAL buffers first for performance, then falls back to disk-based WAL files when necessary.

The function also handles file reloading scenarios during recovery where WAL files might be replaced from archive, implementing retry logic to ensure data consistency.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- : Signal to transition to stopping state
- : Flag indicating streaming completion
- : Whether streaming a historic timeline
- : Whether this is a cascading WAL sender
- : Last WAL position successfully sent

## Dependencies
- Functions called/Symbols referenced:
  - [WalSndSetState](../W/WalSndSetState.md)
  - [GetStandbyFlushRecPtr](../G/GetStandbyFlushRecPtr.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetWALInsertionTimeLine](../G/GetWALInsertionTimeLine.md)
  - [readTimeLineHistory](../r/readTimeLineHistory.md)
  - [tliSwitchPoint](../t/tliSwitchPoint.md)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - [LagTrackerWrite](../L/LagTrackerWrite.md)
  - [WALReadFromBuffers](../W/WALReadFromBuffers.md)
  - [WALRead](../W/WALRead.md)
  - [CheckXLogRemoved](../C/CheckXLogRemoved.md)
  - [wal_segment_close](../w/wal_segment_close.md)
  - pq_putmessage_noblock
- Called from (representative examples):
  - [StartReplication](../S/StartReplication.md)

## Notes and Other Information
- Implements lag tracking by recording timestamps for WAL positions to measure replication lag
- Ensures WAL records are never split across messages by aligning to page boundaries
- Handles the CopyDone protocol message when reaching end of historic timelines
- Updates shared memory status and process title to reflect streaming progress
- Uses spinlocks for safe concurrent access to WAL sender shared state
- Implements retry logic for file reloading scenarios during cascading replication
- Maintains strict durability guarantees by only streaming fsynced WAL on primaries

## Simplified Source

```c
// Simplified version of XLogSendPhysical
static void XLogSendPhysical(void) {
    XLogRecPtr SendRqstPtr;
    XLogRecPtr startptr, endptr;
    Size nbytes, rbytes;
    XLogSegNo segno;
    WALReadError errinfo;

    // Handle stopping request
    if (got_STOPPING)
        WalSndSetState(WALSNDSTATE_STOPPING);

    if (streamingDoneSending) {
        WalSndCaughtUp = true;
        return;
    }

    // Determine how far we can safely send WAL
    if (sendTimeLineIsHistoric) {
        // Historic timeline: send up to switch point
        SendRqstPtr = sendTimeLineValidUpto;
    } else if (am_cascading_walsender) {
        // Cascading standby: send replayed/received WAL
        TimeLineID SendRqstTLI;
        bool becameHistoric = false;

        SendRqstPtr = GetStandbyFlushRecPtr(&SendRqstTLI);

        // Check for promotion or timeline change
        if (!RecoveryInProgress()) {
            SendRqstTLI = GetWALInsertionTimeLine();
            am_cascading_walsender = false;
            becameHistoric = true;
        } else if (sendTimeLine != SendRqstTLI) {
            becameHistoric = true;
        }

        // Handle timeline becoming historic
        if (becameHistoric) {
            List *history = readTimeLineHistory(SendRqstTLI);
            sendTimeLineValidUpto = tliSwitchPoint(sendTimeLine, history, &sendTimeLineNextTLI);
            list_free_deep(history);
            sendTimeLineIsHistoric = true;
            SendRqstPtr = sendTimeLineValidUpto;
        }
    } else {
        // Primary: send all flushed WAL
        SendRqstPtr = GetFlushRecPtr(NULL);
    }

    // Record lag tracking information
    LagTrackerWrite(SendRqstPtr, GetCurrentTimestamp());

    // Check if we've reached end of historic timeline
    if (sendTimeLineIsHistoric && sendTimeLineValidUpto <= sentPtr) {
        if (xlogreader->seg.ws_file >= 0)
            wal_segment_close(xlogreader);
        pq_putmessage_noblock('c', NULL, 0);  // Send CopyDone
        streamingDoneSending = true;
        WalSndCaughtUp = true;
        return;
    }

    // Check if we have work to do
    if (SendRqstPtr <= sentPtr) {
        WalSndCaughtUp = true;
        return;
    }

    // Calculate data size to send (max MAX_SEND_SIZE, aligned to boundaries)
    startptr = sentPtr;
    endptr = startptr + MAX_SEND_SIZE;
    if (SendRqstPtr <= endptr) {
        endptr = SendRqstPtr;
        WalSndCaughtUp = !sendTimeLineIsHistoric;
    } else {
        endptr -= (endptr % XLOG_BLCKSZ);  // Align to page boundary
        WalSndCaughtUp = false;
    }

    nbytes = endptr - startptr;

    // Prepare output message
    resetStringInfo(&output_message);
    pq_sendbyte(&output_message, 'w');           // WAL data message
    pq_sendint64(&output_message, startptr);     // dataStart
    pq_sendint64(&output_message, SendRqstPtr);  // walEnd
    pq_sendint64(&output_message, 0);            // sendtime placeholder

    // Read WAL data
    enlargeStringInfo(&output_message, nbytes);

retry:
    // Try WAL buffers first, then files
    rbytes = WALReadFromBuffers(&output_message.data[output_message.len],
                               startptr, nbytes, xlogreader->seg.ws_tli);
    output_message.len += rbytes;
    startptr += rbytes;
    nbytes -= rbytes;

    // Read remaining from WAL files
    if (nbytes > 0 && !WALRead(xlogreader, &output_message.data[output_message.len],
                              startptr, nbytes, xlogreader->seg.ws_tli, &errinfo))
        WALReadRaiseError(&errinfo);

    // Validate WAL segment availability
    XLByteToSeg(startptr, segno, xlogreader->segcxt.ws_segsize);
    CheckXLogRemoved(segno, xlogreader->seg.ws_tli);

    // Handle file reloading for cascading senders
    if (am_cascading_walsender) {
        bool reload;
        SpinLockAcquire(&MyWalSnd->mutex);
        reload = MyWalSnd->needreload;
        MyWalSnd->needreload = false;
        SpinLockRelease(&MyWalSnd->mutex);

        if (reload && xlogreader->seg.ws_file >= 0) {
            wal_segment_close(xlogreader);
            goto retry;
        }
    }

    output_message.len += nbytes;
    output_message.data[output_message.len] = '\0';

    // Fill in send timestamp
    resetStringInfo(&tmpbuf);
    pq_sendint64(&tmpbuf, GetCurrentTimestamp());
    memcpy(&output_message.data[1 + sizeof(int64) + sizeof(int64)],
           tmpbuf.data, sizeof(int64));

    // Send the message
    pq_putmessage_noblock('d', output_message.data, output_message.len);
    sentPtr = endptr;

    // Update shared memory status
    SpinLockAcquire(&MyWalSnd->mutex);
    MyWalSnd->sentPtr = sentPtr;
    SpinLockRelease(&MyWalSnd->mutex);

    // Update process title
    if (update_process_title) {
        char activitymsg[50];
        snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X", LSN_FORMAT_ARGS(sentPtr));
        set_ps_display(activitymsg);
    }
}
```

Key simplifications made:
- Condensed timeline logic while preserving all three replication scenarios
- Simplified the WAL reading flow with clear comments
- Maintained all essential protocol message construction
- Preserved critical error handling and validation
- Kept the retry logic for cascading replication scenarios
- Maintained strict boundary alignment for WAL record integrity