# logicalrep_read_stream_start

## Location
[src/backend/replication/logical/proto.c:1087-1102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L1087-L1102)

## Overview
Reads and parses a stream start message from the logical replication input stream, extracting transaction ID and first segment flag information.

## Definition
```c
TransactionId logicalrep_read_stream_start(StringInfo in, bool *first_segment)
```

## Detailed Description
This function deserializes a LOGICAL_REP_MSG_STREAM_START message from the logical replication protocol stream. It extracts the transaction ID of the transaction being streamed and determines whether this is the first segment of the stream for this transaction. This is the receiving counterpart to logicalrep_write_stream_start, enabling the subscriber to properly handle streamed transactions.

The function reads the binary data in the same order it was written: first the 4-byte transaction ID, then the 1-byte first segment flag. The first_segment flag is set to true if the byte value is 1, false otherwise.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming logical replication message data
- `first_segment`: Pointer to boolean that will be set to indicate if this is the first streaming segment

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint (extract 32-bit integer from message buffer)
  - pq_getmsgbyte (extract single byte from message buffer)
  - Assert (debug assertion macro)
- Called from:
  - apply_handle_stream_start (logical replication worker stream start handler)

## Notes and Other Information
- This is a public function in the logical replication protocol API
- Returns the TransactionId of the transaction being streamed
- The function includes an assertion to ensure first_segment pointer is not NULL
- Part of PostgreSQL's streaming replication feature for handling large transactions efficiently
- The subscriber uses this information to properly initialize transaction streaming state
- Enables incremental processing of large transactions to reduce memory usage
- The first_segment flag helps distinguish between new streams and continuation of existing streams