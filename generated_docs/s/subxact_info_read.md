# subxact_info_read

## Location
src/backend/replication/logical/worker.c: 4068 - 4118

## Overview
Restores subtransaction information from a file into memory for a streamed logical replication transaction.

## Definition
```c
static void subxact_info_read(Oid subid, TransactionId xid)
```

## Detailed Description
This function reads previously stored subtransaction information from a file back into the global subxact_data structure. It handles the complete restoration process, including memory allocation in the appropriate context (LogicalStreamingContext), file validation, and proper sizing of internal data structures. The function is designed to work with files created by subxact_info_write and gracefully handles the case where no subtransaction file exists.

## Parameters / Member Variables
- `subid`: Object ID of the subscription
- `xid`: Transaction ID of the toplevel transaction

## Dependencies
- Functions called/Symbols referenced:
  - [subxact_filename](subxact_filename.md)
  - BufFileOpenFileSet
  - BufFileReadExact
  - BufFileClose
  - [my_log2](../m/my_log2.md)
  - [palloc](../p/palloc.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - SubXactInfo
  - BufFile
  - LogicalStreamingContext
- Called from (representative examples):
  - [stream_start_internal](stream_start_internal.md)
  - [stream_abort_internal](stream_abort_internal.md)

## Notes and Other Information
- This is a static function with internal linkage within worker.c
- The function includes assertions to ensure the subxact_data structure is in a clean state before reading
- Memory allocation occurs in LogicalStreamingContext to persist throughout the streaming session
- The maximum number of subtransactions is kept as a power of 2 for efficient memory management
- If no subtransaction file exists, the function returns early without error
- The function uses BufFileReadExact for reliable file reading operations
- Memory allocated here is later freed by cleanup_subxact_info() when the stream completes
- The implementation handles the case where len > 0 before reading actual subtransaction data