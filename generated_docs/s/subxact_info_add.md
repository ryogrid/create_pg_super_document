# subxact_info_add

## Location
[src/backend/replication/logical/worker.c:4119-4196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4119-L4196)

## Overview
Adds information about a subtransaction, specifically its offset in the main stream file, to the in-memory subtransaction tracking structure.

## Definition
```c
static void subxact_info_add(TransactionId xid)
```

## Detailed Description
This function manages the dynamic tracking of subtransactions during logical replication streaming. It maintains an array of SubXactInfo structures that record the file position where each subtransaction's first change appears in the stream file. The function implements several optimizations: it avoids duplicate entries, skips the toplevel transaction, and uses reverse linear search since recent subtransactions are more likely to be accessed. Memory management is handled dynamically with initial allocation of 128 entries and doubling when capacity is exceeded.

## Parameters / Member Variables
- `xid`: Transaction ID of the subtransaction to add

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - BufFileTell
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - SubXactInfo
  - LogicalStreamingContext
  - stream_xid (global variable)
  - stream_fd (global variable)
  - subxact_data (global structure)
- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)

## Notes and Other Information
- This is a static function with internal linkage within worker.c
- The function includes several performance optimizations:
  - Early return for toplevel transaction XIDs
  - Caching of last processed XID to avoid redundant processing
  - Reverse linear search from array tail for better cache locality
- Memory allocation occurs in LogicalStreamingContext for proper lifetime management
- Initial capacity is set to 128 SubXactInfo entries and doubles when full
- The function stores both file number and offset using BufFileTell for precise positioning
- Contains a TODO comment suggesting binary search optimization if XIDs arrive in sorted order
- Array scanning is intentionally done in reverse order since recent subtransactions are more likely to be accessed again