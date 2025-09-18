# DecodeTruncate

## Location
[src/backend/replication/logical/decode.c:1086-1123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L1086-L1123)

## Overview
DecodeTruncate is a function that parses XLOG_HEAP_TRUNCATE records from the write-ahead log (WAL) during logical replication, converting them into reorder buffer changes for output plugins.

## Definition


## Detailed Description
DecodeTruncate processes heap truncate operations recorded in the WAL during logical decoding. When a TRUNCATE statement is executed on a table, it generates an XLOG_HEAP_TRUNCATE WAL record. This function extracts the relevant information from that record and creates a corresponding change entry in the reorder buffer.

The function performs several important checks:
1. Filters by database ID to ensure only records from the target database are processed
2. Applies origin filtering if configured to ignore certain replication origins
3. Extracts truncate-specific flags (CASCADE and RESTART SEQUENCES options)
4. Copies the list of relation OIDs that were truncated
5. Queues the change in the reorder buffer for eventual delivery to output plugins

## Parameters / Member Variables
- : LogicalDecodingContext containing the decoding session state, including the replication slot and reorder buffer
- : XLogRecordBuffer containing the WAL record being decoded and its metadata

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [FilterByOrigin](../F/FilterByOrigin.md)
  - XLogRecGetOrigin
  - [ReorderBufferGetChange](../R/ReorderBufferGetChange.md)
  - [ReorderBufferGetRelids](../R/ReorderBufferGetRelids.md)
  - [ReorderBufferQueueChange](../R/ReorderBufferQueueChange.md)
  - XLogRecGetXid
- Called from (representative examples):
  - [heap_decode](../h/heap_decode.md)

## Notes and Other Information
- This function is part of PostgreSQL's logical replication infrastructure
- Only processes records from the database specified in the replication slot
- Handles both CASCADE and RESTART SEQUENCES truncate options through flag checking
- The function is static, meaning it's only accessible within the decode.c compilation unit
- Truncate operations can affect multiple relations simultaneously, hence the relids array handling
- Origin filtering allows selective replication based on the source of changes