# logicalrep_write_stream_abort

## Location
[src/backend/replication/logical/proto.c:1166-1191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L1166-L1191)

## Overview
Writes a stream abort message to the logical replication output stream to signal the abortion of a streaming transaction or subtransaction.

## Definition
void logicalrep_write_stream_abort(StringInfo out, TransactionId xid, TransactionId subxid, XLogRecPtr abort_lsn, TimestampTz abort_time, bool write_abort_info)

## Detailed Description
This function is part of PostgreSQL's logical replication protocol implementation for streaming transactions. It writes a LOGICAL_REP_MSG_STREAM_ABORT message that contains information about an aborted transaction or subtransaction. The function supports both top-level transaction aborts (where xid and subxid are the same) and subtransaction aborts.

The function includes a conditional mechanism to optionally include abort LSN and timestamp information based on the write_abort_info parameter. This allows for flexible message formatting depending on the specific abort scenario and protocol requirements.

## Parameters / Member Variables
- `out`: StringInfo buffer where the stream abort message will be written
- `xid`: TransactionId of the main transaction being aborted
- `subxid`: TransactionId of the subtransaction being aborted (same as xid for top-level aborts)
- `abort_lsn`: XLogRecPtr indicating the LSN where the abort occurred
- `abort_time`: TimestampTz indicating when the abort occurred
- `write_abort_info`: Boolean flag controlling whether to include abort_lsn and abort_time in the message

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - LOGICAL_REP_MSG_STREAM_ABORT (message type constant)
  - TransactionIdIsValid (assertion)
- Called from (representative examples):
  - [pgoutput_stream_abort](../p/pgoutput_stream_abort.md)

## Notes and Other Information
- For top-level transaction aborts, xid and subxid parameters are the same value
- Includes assertions to ensure both transaction IDs are valid before sending
- Conditionally includes abort LSN and timestamp based on write_abort_info parameter
- Part of the logical replication streaming transaction protocol for handling transaction failures
- Used to notify subscribers that a streaming transaction should be rolled back
- Located in src/backend/replication/logical/proto.c:1166-1191

## Simplified Source

```c
void logicalrep_write_stream_abort(StringInfo out, TransactionId xid,
                                   TransactionId subxid, XLogRecPtr abort_lsn,
                                   TimestampTz abort_time, bool write_abort_info) {
    // Send stream abort message type
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_ABORT);

    // Send transaction IDs (xid and subxid are same for top-level aborts)
    pq_sendint32(out, xid);
    pq_sendint32(out, subxid);

    // Optionally include abort details
    if (write_abort_info) {
        pq_sendint64(out, abort_lsn);    // Where abort occurred
        pq_sendint64(out, abort_time);   // When abort occurred
    }
}
```