# xl_heap_delete

## Location
[src/include/access/heapam_xlog.h:112-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L112-L118)

## Overview
The xl_heap_delete struct represents the WAL (Write-Ahead Logging) record data for heap tuple deletion operations in PostgreSQL's recovery and replication system.

## Definition


## Detailed Description
This structure contains the essential information needed to record and replay heap tuple deletion operations in PostgreSQL's WAL system. When a tuple is deleted from a heap table, this record is written to the WAL to ensure the deletion can be replayed during crash recovery or streamed to replicas for replication. The structure captures the transaction ID that performed the deletion, the location of the deleted tuple, and metadata about the tuple's state.

## Parameters / Member Variables
- : The transaction ID that deleted the tuple, stored in the tuple's xmax field
- : The offset number (position) of the deleted tuple within its page
- : Information mask bits that describe the tuple's state and properties
- : Additional flags providing context about the deletion operation

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md) (creates WAL records using this structure)
  - [heap_abort_speculative](../h/heap_abort_speculative.md) (uses for speculative insertion cleanup)
  - [heap_xlog_delete](../h/heap_xlog_delete.md) (replays deletion from WAL records)
  - [heap_desc](../h/heap_desc.md) (describes WAL records for debugging)
  - [DecodeDelete](../D/DecodeDelete.md) (logical replication decoding)

## Notes and Other Information
- This structure is part of PostgreSQL's WAL record format for heap operations
- The SizeOfHeapDelete macro provides the size of this structure
- Used extensively in crash recovery, point-in-time recovery, and streaming replication
- The infobits_set field preserves important tuple visibility information needed for proper MVCC behavior during replay