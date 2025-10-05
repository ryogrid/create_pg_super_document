# UpdateWorkerStats

## Location
[src/backend/replication/logical/worker.c:3475-3490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3475-L3490)

## Overview
UpdateWorkerStats is a static function that updates the statistics and timestamps of a logical replication worker to track the latest activity and message processing.

## Definition

```c
static void
UpdateWorkerStats(XLogRecPtr last_lsn, TimestampTz send_time, bool reply)
```
## Detailed Description
This function maintains the operational statistics for a logical replication worker by updating key timestamps and LSN (Log Sequence Number) values in the MyLogicalRepWorker global structure. It records when messages were last received and processed, enabling monitoring of replication lag and worker activity. When the reply parameter is true, it additionally updates reply-specific statistics to track the last position that was acknowledged back to the publisher.

## Parameters / Member Variables
- `last_lsn`: The LSN of the last processed WAL record or message
- `send_time`: The timestamp when the message was sent by the publisher
- `reply`: Boolean flag indicating whether this update corresponds to a reply message being sent
## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
- Called from (representative examples):
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (at lines 3598, 3616)

## Notes and Other Information
- This is a static function internal to the worker.c file
- Updates the global MyLogicalRepWorker structure which tracks the current worker's state
- The function sets last_recv_time to the current timestamp, providing a way to measure replication lag
- Reply-specific statistics (reply_lsn and reply_time) are only updated when the reply flag is true, typically when sending acknowledgments back to the publisher

## Simplified Source

```c
static void
UpdateWorkerStats(XLogRecPtr last_lsn, TimestampTz send_time, bool reply)
{
    // Update core worker statistics
    MyLogicalRepWorker->last_lsn = last_lsn;           // Latest processed LSN
    MyLogicalRepWorker->last_send_time = send_time;    // When publisher sent it
    MyLogicalRepWorker->last_recv_time = GetCurrentTimestamp();  // When we received it

    // Update reply statistics if this is a reply message
    if (reply)
    {
        MyLogicalRepWorker->reply_lsn = last_lsn;      // Last LSN we replied about
        MyLogicalRepWorker->reply_time = send_time;    // When that message was sent
    }
}
```