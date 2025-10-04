# logicalrep_write_stream_stop

## Location
[src/backend/replication/logical/proto.c:1103-1111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L1103-L1111)

## Overview
Writes a stream stop message to the logical replication output stream to signal the end of a streaming transaction.

## Definition
void logicalrep_write_stream_stop(StringInfo out)

## Detailed Description
This function is part of PostgreSQL's logical replication protocol implementation. It writes a LOGICAL_REP_MSG_STREAM_STOP message to the output stream to indicate that a streaming transaction has ended. This is a simple protocol message that consists of just the message type identifier without any additional data.

The function is used in the context of streaming logical replication where large transactions are streamed in chunks rather than being buffered entirely in memory before being sent to subscribers.

## Parameters / Member Variables
- `out`: StringInfo buffer where the stream stop message will be written

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - LOGICAL_REP_MSG_STREAM_STOP (message type constant)
- Called from (representative examples):
  - [pgoutput_stream_stop](../p/pgoutput_stream_stop.md)

## Notes and Other Information
- This is a simple protocol message that only sends the message type byte
- Part of the logical replication streaming transaction protocol
- Used to mark the end of a streaming transaction sequence
- Located in src/backend/replication/logical/proto.c:1103-1111

## Simplified Source

```c
void logicalrep_write_stream_stop(StringInfo out)
{
    // Send stream stop message type
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_STOP);
}
```