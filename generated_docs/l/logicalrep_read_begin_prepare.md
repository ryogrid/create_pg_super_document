# logicalrep_read_begin_prepare

## Location
[src/backend/replication/logical/proto.c:145-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L145-L165)

## Overview
Reads and parses a BEGIN PREPARE message from the logical replication input stream to extract prepared transaction metadata.

## Definition
```c
void logicalrep_read_begin_prepare(StringInfo in, LogicalRepPreparedTxnData *begin_data)
```

## Detailed Description
This function deserializes a BEGIN PREPARE message from the logical replication stream, extracting transaction metadata for a prepared transaction. It reads the LSN positions, timing information, transaction ID, and global transaction identifier (GID) from the input buffer. The function includes validation to ensure critical LSN fields are properly set, throwing errors if invalid values are encountered. This is the counterpart to logicalrep_write_begin_prepare for the receiving side of logical replication.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the serialized message to be parsed
- `begin_data`: LogicalRepPreparedTxnData structure where the parsed transaction data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [pq_getmsgstring](../p/pq_getmsgstring.md)
  - [strlcpy](../s/strlcpy.md)
  - elog
  - InvalidXLogRecPtr
  - [LogicalRepPreparedTxnData](../L/LogicalRepPreparedTxnData.md)
- Called from (representative examples):
  - [apply_handle_begin_prepare](../a/apply_handle_begin_prepare.md)

## Notes and Other Information
- Validates that prepare_lsn and end_lsn are not InvalidXLogRecPtr, throwing errors if they are
- Uses strlcpy to safely copy the GID string into a pre-allocated buffer
- Part of the logical replication protocol for two-phase commit support
- Reads the same fields written by logicalrep_write_begin_prepare in the same order
- Located in src/backend/replication/logical/proto.c:145-165