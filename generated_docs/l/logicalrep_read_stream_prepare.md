# logicalrep_read_stream_prepare

## Location
[src/backend/replication/logical/proto.c:376-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L376-L384)

## Overview
This function reads a STREAM PREPARE message from the logical replication input stream, parsing the preparation data for a streamed transaction in PostgreSQL's two-phase commit protocol.

## Definition

```c
void
logicalrep_read_stream_prepare(StringInfo in, LogicalRepPreparedTxnData *prepare_data)
```
## Detailed Description
The  function is the counterpart to , responsible for deserializing STREAM PREPARE messages from the logical replication stream. It delegates the parsing work to  with "stream prepare" as the message type identifier. This function extracts transaction preparation information including LSN positions, prepare timestamp, transaction ID, and global identifier from the input stream and populates the provided  structure.

The function performs validation on the received data, ensuring that required fields like prepare_lsn, end_lsn, and transaction ID are valid, and that no unrecognized flags are present in the message.

## Parameters / Member Variables
- : StringInfo buffer containing the serialized STREAM PREPARE message to be parsed
- : LogicalRepPreparedTxnData structure to be populated with the parsed preparation information, including prepare_lsn, end_lsn, prepare_time, xid, and gid

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_read_prepare_common](logicalrep_read_prepare_common.md)
  - [LogicalRepPreparedTxnData](../L/LogicalRepPreparedTxnData.md) (type)
- Called from (representative examples):
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)

## Notes and Other Information
- This function is the read counterpart to logicalrep_write_stream_prepare
- Performs extensive validation of received data to ensure message integrity
- The prepare_data structure must be pre-allocated by the caller
- Part of the logical replication protocol message parsing system
- Located in src/backend/replication/logical/proto.c:376-384