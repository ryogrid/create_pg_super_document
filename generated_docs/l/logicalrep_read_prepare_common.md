# logicalrep_read_prepare_common

## Location
[src/backend/replication/logical/proto.c:210-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L210-L238)

## Overview
A static helper function that provides core functionality for reading PREPARE messages in logical replication, shared between regular and streaming prepare operations.

## Definition
```c
static void logicalrep_read_prepare_common(StringInfo in, char *msgtype, LogicalRepPreparedTxnData *prepare_data)
```

## Detailed Description
This internal function encapsulates the common logic for deserializing PREPARE messages in logical replication. It handles both regular PREPARE and STREAM PREPARE messages by accepting a message type string for error reporting. The function reads and validates flags, LSN positions, timing information, transaction ID, and global transaction identifier from the input stream. It includes comprehensive validation with specific error messages that include the message type context, ensuring robust error handling across different prepare message variants.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the serialized message to be parsed
- `msgtype`: String describing the message type for error reporting (e.g., "prepare", "stream prepare")
- `prepare_data`: LogicalRepPreparedTxnData structure where the parsed transaction data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [pq_getmsgstring](../p/pq_getmsgstring.md)
  - strlcpy
  - elog
  - InvalidXLogRecPtr
  - InvalidTransactionId
  - [LogicalRepPreparedTxnData](../L/LogicalRepPreparedTxnData.md)
- Called from (representative examples):
  - logicalrep_read_prepare
  - [logicalrep_read_stream_prepare](logicalrep_read_stream_prepare.md)

## Notes and Other Information
- Static function internal to proto.c, not exposed in header files
- Validates that flags field is 0, rejecting any non-zero flags as unrecognized
- Comprehensive validation of LSN and transaction ID fields with descriptive error messages
- Uses strlcpy to safely copy the GID string into a pre-allocated buffer
- Message type parameter allows reuse while providing context-specific error messages
- Located in src/backend/replication/logical/proto.c:210-238