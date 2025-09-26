# XLogReaderHasQueuedRecordOrError

## Location
src/include/access/xlogreader.h: 325 - 348

## Overview
A static inline function that checks whether an XLogReaderState has queued decoded records or a deferred error message that can be returned by XLogNextRecord().

## Definition

```c
typedef enum XLogPageReadResult
{
	XLREAD_SUCCESS = 0,			/* record is successfully read */
	XLREAD_FAIL = -1,			/* failed during reading a record */
	XLREAD_WOULDBLOCK = -2,		/* nonblocking mode only, no data */
} XLogPageReadResult;
```
## Detailed Description
This function provides a quick check to determine if there are any pre-decoded WAL records or deferred error messages waiting to be processed in the XLogReaderState. It examines two key fields:

1. **decode_queue_head**: Points to the oldest decoded record in the queue of pre-processed WAL records. When non-NULL, it indicates that decoded records are available for immediate consumption without needing to read and decode additional WAL data.

2. **errormsg_deferred**: A boolean flag indicating that an error occurred during previous processing but was deferred (not immediately reported). When true, it means the next call to XLogNextRecord() should return this error instead of trying to read new data.

The function is primarily used by the WAL prefetching subsystem to implement non-blocking behavior. When queued records or errors are available, the system can avoid blocking operations and instead process the already-available data.

This optimization is particularly important for:
- WAL prefetching operations that need to determine whether to block waiting for new data
- Recovery processes that want to maximize throughput by processing available records first
- Systems that need to implement non-blocking WAL reading patterns

## Parameters / Member Variables
- : Pointer to the XLogReaderState structure containing the WAL reader's current state, including the decode queue and error status

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses struct fields directly)
- Called from (representative examples):
  - XLogPrefetcherNextBlock (src/backend/access/transam/xlogprefetcher.c:486)
  - XLogPrefetcherReadRecord (src/backend/access/transam/xlogprefetcher.c:1043)
  - XLogReadRecord (src/backend/access/transam/xlogreader.c:403)

## Notes and Other Information
- This is a static inline function defined in the header file for performance reasons, as it performs only simple field access operations
- The function implements a core optimization for WAL reading performance by allowing consumers to check for available work without expensive I/O operations
- Used extensively in the WAL prefetcher subsystem to implement efficient non-blocking record reading
- The decode queue mechanism allows multiple WAL records to be decoded in advance, improving overall system throughput during recovery and replication
- The deferred error mechanism ensures that errors are properly propagated even when using asynchronous/queued processing patterns