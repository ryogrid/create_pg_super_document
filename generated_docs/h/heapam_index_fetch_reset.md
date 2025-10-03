# heapam_index_fetch_reset

## Location
[src/backend/access/heap/heapam_handler.c:91-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L91-L102)

## Overview
Resets the heap index fetch scan state by releasing any currently held buffer and preparing the scan for reuse or termination.

## Definition

```c
static void
heapam_index_fetch_reset(IndexFetchTableData *scan)
```
## Detailed Description
This function provides cleanup and reset functionality for heap index fetch operations within PostgreSQL's table access method framework. It takes an IndexFetchTableData pointer (which is actually an IndexFetchHeapData structure) and performs necessary cleanup by releasing any buffer that is currently pinned by the scan. The function checks if there is a valid buffer held in xs_cbuf, and if so, releases it using ReleaseBuffer() and resets the buffer reference to InvalidBuffer. This ensures proper resource management and prevents buffer leaks during index scan operations.

## Parameters / Member Variables
- `*scan`: Pointer to IndexFetchTableData structure (cast internally to IndexFetchHeapData) representing the index fetch scan state
## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md) (macro to check buffer validity)
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (function to release a pinned buffer)
  - InvalidBuffer (constant representing invalid buffer state)
  - [IndexFetchHeapData](../I/IndexFetchHeapData.md) (heap-specific index fetch data structure)
- Called from (representative examples):
  - [heapam_index_fetch_end](heapam_index_fetch_end.md) (cleanup during scan termination)
  - Part of TableAmRoutine structure as a callback function

## Notes and Other Information
- This function is essential for proper buffer management in PostgreSQL
- Can be called multiple times safely due to the BufferIsValid check
- Part of the index fetch operation lifecycle (begin, fetch, reset, end)
- Prevents buffer leaks by ensuring pinned buffers are properly released
- The function is idempotent - calling it multiple times has no adverse effects

## Simplified Source

```c
static void heapam_index_fetch_reset(IndexFetchTableData *scan) {
    IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;

    // Release any currently held buffer
    if (BufferIsValid(hscan->xs_cbuf)) {
        ReleaseBuffer(hscan->xs_cbuf);
        hscan->xs_cbuf = InvalidBuffer;
    }
}
```