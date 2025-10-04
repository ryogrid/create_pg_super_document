# logicalrep_write_stream_start

## Location
[src/backend/replication/logical/proto.c:1069-1086](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L1069-L1086)

## Overview
Writes a stream start message to the logical replication output stream, indicating the beginning of streaming for a specific transaction.

## Definition
```c
void logicalrep_write_stream_start(StringInfo out, TransactionId xid, bool first_segment)
```

## Detailed Description
This function serializes a LOGICAL_REP_MSG_STREAM_START message into the logical replication protocol stream. It marks the beginning of streaming mode for a specific transaction, which is used when large transactions need to be sent incrementally rather than all at once. The message includes the transaction ID being streamed and a flag indicating whether this is the first segment of the stream for this transaction.

Streaming mode allows PostgreSQL to send large transactions in smaller chunks, reducing memory usage and improving performance for both publisher and subscriber. The first_segment flag helps the receiver distinguish between the initial stream start and continuation of an existing stream.

## Parameters / Member Variables
- `out`: StringInfo buffer to write the stream start message to
- `xid`: Transaction ID of the transaction being streamed (must be valid)
- `first_segment`: Boolean flag indicating if this is the first streaming segment for this transaction

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md) (write single byte to message buffer)
  - [pq_sendint32](../p/pq_sendint32.md) (write 32-bit integer to message buffer)
  - LOGICAL_REP_MSG_STREAM_START (message type constant)
  - TransactionIdIsValid (assertion to validate transaction ID)
  - Assert (debug assertion macro)
- Called from:
  - [pgoutput_stream_start](../p/pgoutput_stream_start.md) (pgoutput plugin stream start handler)

## Notes and Other Information
- This is a public function in the logical replication protocol API
- The function includes an assertion to ensure the transaction ID is valid before streaming
- Part of PostgreSQL's streaming replication feature for handling large transactions efficiently
- The message format includes: message type byte, transaction ID (4 bytes), and first segment flag (1 byte)
- Streaming allows large transactions to be processed incrementally, reducing memory pressure on both publisher and subscriber
- The first_segment flag enables proper transaction state management on the receiving side

## Simplified Source

```c
void logicalrep_write_stream_start(StringInfo out, TransactionId xid, bool first_segment)
{
    // Send message type
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_START);

    // Validate and send transaction ID
    Assert(TransactionIdIsValid(xid));
    pq_sendint32(out, xid);

    // Send first segment flag
    pq_sendbyte(out, first_segment ? 1 : 0);
}
```