# send_feedback

## Location
[src/backend/replication/logical/worker.c:3755-3843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3755-L3843)

## Overview
send_feedback sends a Standby Status Update message to the publisher to communicate the apply worker's progress in processing logical replication data.

## Definition

```c
static void
send_feedback(XLogRecPtr recvpos, bool force, bool requestReply)
```
## Detailed Description
This function constructs and sends a replication protocol feedback message ('r' message type) to inform the publisher about the logical replication worker's current progress. It tracks the latest LSN positions for received, written, flushed, and applied data. The function implements intelligent throttling by only sending feedback when necessary (based on time intervals or forced sends) and maintains static variables to track the last reported positions. It communicates three key LSN positions: write (latest received), flush (latest flushed to disk), and apply (latest applied), along with timestamps and reply request flags.

## Parameters / Member Variables
- `recvpos`: The latest LSN position that has been received from the publisher
- `force`: Boolean flag to force sending feedback regardless of time intervals (used for timeouts and mandatory responses)
- `requestReply`: Boolean flag indicating whether the publisher should send a reply to this feedback message
## Dependencies
- Functions called/Symbols referenced:
  - [get_flush_position](../g/get_flush_position.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [TimestampDifferenceExceeds](../T/TimestampDifferenceExceeds.md)
  - [makeStringInfo](../m/makeStringInfo.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - walrcv_send
- Called from (representative examples):
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (at lines 3615, 3628, 3725)

## Notes and Other Information
- This is a static function internal to the worker.c file
- Uses static variables to track last reported positions and avoid redundant feedback messages
- Respects the wal_receiver_status_interval setting to throttle feedback frequency
- For synchronous replication, reports the received position as both write and flush when there are no pending transactions
- Constructs protocol messages using pq_* functions for binary message formatting
- The 'r' message type follows the logical replication protocol specification
- Includes debug logging to trace feedback messages being sent
- Maintains three different LSN positions: received data, written/flushed data, and applied data

## Simplified Source

```c
static void
send_feedback(XLogRecPtr recvpos, bool force, bool requestReply)
{
    static StringInfo reply_message = NULL;
    static TimestampTz send_time = 0;
    static XLogRecPtr last_recvpos = InvalidXLogRecPtr;
    static XLogRecPtr last_writepos = InvalidXLogRecPtr;
    static XLogRecPtr last_flushpos = InvalidXLogRecPtr;

    XLogRecPtr writepos, flushpos;
    TimestampTz now;
    bool have_pending_txes;

    // Exit early if status reporting is disabled and not forced
    if (!force && wal_receiver_status_interval <= 0)
        return;

    // Ensure recvpos doesn't go backwards
    if (recvpos < last_recvpos)
        recvpos = last_recvpos;

    // Get current flush positions from WAL system
    get_flush_position(&writepos, &flushpos, &have_pending_txes);

    // For synchronous replication: if no pending transactions,
    // report received position as both write and flush
    if (!have_pending_txes)
        flushpos = writepos = recvpos;

    // Ensure positions don't go backwards
    if (writepos < last_writepos)
        writepos = last_writepos;
    if (flushpos < last_flushpos)
        flushpos = last_flushpos;

    now = GetCurrentTimestamp();

    // Skip sending if nothing changed and not enough time passed
    if (!force &&
        writepos == last_writepos &&
        flushpos == last_flushpos &&
        !TimestampDifferenceExceeds(send_time, now,
                                   wal_receiver_status_interval * 1000))
        return;

    send_time = now;

    // Initialize or reset the reply message buffer
    if (!reply_message) {
        MemoryContext oldctx = MemoryContextSwitchTo(ApplyContext);
        reply_message = makeStringInfo();
        MemoryContextSwitchTo(oldctx);
    } else {
        resetStringInfo(reply_message);
    }

    // Build the 'r' (standby status update) protocol message
    pq_sendbyte(reply_message, 'r');
    pq_sendint64(reply_message, recvpos);    // write position
    pq_sendint64(reply_message, flushpos);   // flush position
    pq_sendint64(reply_message, writepos);   // apply position
    pq_sendint64(reply_message, now);        // timestamp
    pq_sendbyte(reply_message, requestReply); // reply requested flag

    // Debug logging
    elog(DEBUG2, "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X",
         force, LSN_FORMAT_ARGS(recvpos), LSN_FORMAT_ARGS(writepos),
         LSN_FORMAT_ARGS(flushpos));

    // Send the message to publisher
    walrcv_send(LogRepWorkerWalRcvConn, reply_message->data, reply_message->len);

    // Update last reported positions
    if (recvpos > last_recvpos)
        last_recvpos = recvpos;
    if (writepos > last_writepos)
        last_writepos = writepos;
    if (flushpos > last_flushpos)
        last_flushpos = flushpos;
}
```