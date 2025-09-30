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
  - [list_delete_first](../l/list_delete_first.md), lappend (list manipulation functions)
  - [CopyMultiInsertBufferCleanup](CopyMultiInsertBufferCleanup.md) (cleanup of removed buffers)
- Called from (representative examples):
  - [CopyFrom](CopyFrom.md) (at src/backend/commands/copyfrom.c:1092)
  - [CopyFrom](CopyFrom.md) (at src/backend/commands/copyfrom.c:1230)
  - [CopyFrom](CopyFrom.md) (at src/backend/commands/copyfrom.c:1304)

## Notes and Other Information
The buffer trimming mechanism is crucial for preventing quadratic memory growth when copying into highly partitioned tables. By limiting buffers to MAX_PARTITION_BUFFERS and removing the oldest first, it maintains reasonable memory usage while preserving performance. The special handling of curr_rri ensures that the currently active buffer is never prematurely removed, which would require immediate recreation and reduce efficiency.

## Simplified Source

```c
static inline void CopyMultiInsertInfoFlush(CopyMultiInsertInfo *miinfo,
                                           ResultRelInfo *curr_rri,
                                           int64 *processed) {
    ListCell *lc;

    // Flush all buffers to their respective tables
    foreach(lc, miinfo->multiInsertBuffers) {
        CopyMultiInsertBuffer *buffer = (CopyMultiInsertBuffer *) lfirst(lc);
        CopyMultiInsertBufferFlush(miinfo, buffer, processed);
    }

    // Reset counters since all tuples are now flushed
    miinfo->bufferedTuples = 0;
    miinfo->bufferedBytes = 0;

    // Trim buffer list to prevent excessive memory usage
    while (list_length(miinfo->multiInsertBuffers) > MAX_PARTITION_BUFFERS) {
        CopyMultiInsertBuffer *buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);

        // Protect currently active buffer by moving it to end if needed
        if (buffer->resultRelInfo == curr_rri) {
            miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
            miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
            buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);
        }

        // Remove and cleanup the oldest buffer
        CopyMultiInsertBufferCleanup(miinfo, buffer);
        miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
    }
}
```