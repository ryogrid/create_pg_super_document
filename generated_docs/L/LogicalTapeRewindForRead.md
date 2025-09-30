# LogicalTapeRewindForRead

## Location
[src/backend/utils/sort/logtape.c:846-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L846-L927)

## Overview
Rewinds a logical tape to the beginning and transitions it from writing mode to reading mode, with configurable buffer size optimization.

## Definition
```c
void LogicalTapeRewindForRead(LogicalTape *lt, size_t buffer_size)
```

## Detailed Description
The `LogicalTapeRewindForRead` function is responsible for transitioning a LogicalTape from write mode to read mode. This involves several critical operations: flushing any remaining data in the write buffer, switching the tape's operational state, optimizing the buffer size for reading, and cleaning up resources like preallocation lists.

When transitioning from writing, the function first ensures that any dirty data in the buffer is written to storage using `ltsWriteBlock`. It handles the special case where very small amounts of data might not have filled the buffer even once, using Valgrind macros to mark memory as defined to avoid false warnings.

For frozen tapes (those that have been previously written and are being rewound for another read pass), the function uses a fixed BLCKSZ buffer size. For regular tapes, it allows the caller to specify a buffer size that will be rounded down to BLCKSZ boundaries and capped at reasonable limits. The function also releases any preallocated blocks back to the tape set's free list, optimizing memory usage.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape to rewind
- `buffer_size`: Desired size for the read buffer (will be adjusted to constraints)

## Dependencies
- Functions called/Symbols referenced:
  - [ltsWriteBlock](../l/ltsWriteBlock.md) (writes final block during transition from writing)
  - [ltsReleaseBlock](../l/ltsReleaseBlock.md) (returns preallocated blocks to free list)
  - TapeBlockSetNBytes (sets byte count in block trailer)
  - VALGRIND_MAKE_MEM_DEFINED (marks memory as defined for Valgrind)
  - [pfree](../p/pfree.md) (frees memory)
  - [LogicalTape](LogicalTape.md) (structure type)
  - [LogicalTapeSet](LogicalTapeSet.md) (structure type)
- Called from (representative examples):
  - [hashagg_spill_finish](../h/hashagg_spill_finish.md)
  - [mergeruns](../m/mergeruns.md)
  - [tuplesort_rescan](../t/tuplesort_rescan.md)

## Notes and Other Information
- Can handle both writing-to-reading transitions and frozen tape rewinds
- Buffer size is constrained to be between BLCKSZ and MaxAllocSize
- Buffer size must be a multiple of BLCKSZ (rounded down if necessary)
- Frozen tapes always use BLCKSZ-sized buffers regardless of buffer_size parameter
- Handles special Valgrind integration for small data scenarios in parallel sorts
- Cleans up preallocation lists and returns unused blocks to improve memory efficiency
- Uses lazy buffer allocation - actual buffer memory is allocated later when needed
- Essential function for external sorting algorithms that need to read previously written data

## Simplified Source

```c
void LogicalTapeRewindForRead(LogicalTape *lt, size_t buffer_size) {
    LogicalTapeSet *lts = lt->tapeSet;

    // Adjust buffer size based on tape state
    if (lt->frozen) {
        buffer_size = BLCKSZ;
    } else {
        // Ensure buffer is at least BLCKSZ and at most max_size
        if (buffer_size < BLCKSZ)
            buffer_size = BLCKSZ;
        if (buffer_size > lt->max_size)
            buffer_size = lt->max_size;

        // Round down to BLCKSZ boundary
        buffer_size -= buffer_size % BLCKSZ;
    }

    // Handle transition from writing to reading
    if (lt->writing) {
        // Flush any remaining data in the buffer
        if (lt->dirty) {
            TapeBlockSetNBytes(lt->buffer, lt->nbytes);
            ltsWriteBlock(lt->tapeSet, lt->curBlockNumber, lt->buffer);
        }
        lt->writing = false;
    } else {
        // Must be a frozen tape for rewind
        Assert(lt->frozen);
    }

    // Clean up current buffer and set new size
    if (lt->buffer)
        pfree(lt->buffer);
    lt->buffer = NULL;
    lt->buffer_size = buffer_size;

    // Release preallocated blocks back to free list
    if (lt->prealloc != NULL) {
        for (int i = lt->nprealloc; i > 0; i--)
            ltsReleaseBlock(lts, lt->prealloc[i - 1]);
        pfree(lt->prealloc);
        lt->prealloc = NULL;
        lt->nprealloc = 0;
        lt->prealloc_size = 0;
    }
}
```