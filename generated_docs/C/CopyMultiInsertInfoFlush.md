# CopyMultiInsertInfoFlush

## Location
[src/backend/commands/copyfrom.c:520-566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L520-L566)

## Overview
Flushes all buffered tuples across all CopyMultiInsertBuffers and manages buffer list trimming to prevent excessive memory usage during partitioned table operations.

## Definition
```c
static inline void CopyMultiInsertInfoFlush(CopyMultiInsertInfo *miinfo, 
                                           ResultRelInfo *curr_rri,
                                           int64 *processed)
```

## Detailed Description
This function coordinates the flushing of all buffered tuples across multiple partition buffers in a CopyMultiInsertInfo structure. It performs two main operations:

1. **Buffer Flushing**: Iterates through all CopyMultiInsertBuffers in the multiInsertBuffers list and calls CopyMultiInsertBufferFlush for each one to write their buffered tuples to the respective tables.

2. **Buffer Management**: After flushing, it trims the buffer list to prevent memory bloat by removing the oldest buffers when the list exceeds MAX_PARTITION_BUFFERS (32). The trimming algorithm preserves the currently active buffer (curr_rri) by moving it to the end of the list if it would otherwise be removed.

The function resets the global counters (bufferedTuples and bufferedBytes) to zero after flushing, since all tuples have been written out.

## Parameters / Member Variables
- `miinfo`: Pointer to CopyMultiInsertInfo containing all buffers and copy operation state
- `curr_rri`: Pointer to ResultRelInfo currently being used, protected from removal during buffer trimming
- `processed`: Pointer to counter tracking total processed tuples, updated by the flush operations

## Dependencies
- Functions called/Symbols referenced:
  - [CopyMultiInsertBufferFlush](CopyMultiInsertBufferFlush.md) (flushes individual buffers)
  - MAX_PARTITION_BUFFERS (constant defining maximum buffer count: 32)
  - list_delete_first, lappend (list manipulation functions)
  - [CopyMultiInsertBufferCleanup](CopyMultiInsertBufferCleanup.md) (cleanup of removed buffers)
- Called from (representative examples):
  - [CopyFrom](CopyFrom.md) (at src/backend/commands/copyfrom.c:1092)
  - [CopyFrom](CopyFrom.md) (at src/backend/commands/copyfrom.c:1230)
  - [CopyFrom](CopyFrom.md) (at src/backend/commands/copyfrom.c:1304)

## Notes and Other Information
The buffer trimming mechanism is crucial for preventing quadratic memory growth when copying into highly partitioned tables. By limiting buffers to MAX_PARTITION_BUFFERS and removing the oldest first, it maintains reasonable memory usage while preserving performance. The special handling of curr_rri ensures that the currently active buffer is never prematurely removed, which would require immediate recreation and reduce efficiency.