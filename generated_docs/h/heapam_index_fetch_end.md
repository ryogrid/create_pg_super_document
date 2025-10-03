# heapam_index_fetch_end

## Location
[src/backend/access/heap/heapam_handler.c:103-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L103-L112)

## Overview
Finalizes and cleans up a heap index fetch scan by resetting the scan state and freeing the allocated memory.

## Definition

```c
static void
heapam_index_fetch_end(IndexFetchTableData *scan)
```
## Detailed Description
This function serves as the cleanup and termination callback for index fetch operations on heap tables within PostgreSQL's table access method framework. It performs the final cleanup steps required when ending an index scan, including calling heapam_index_fetch_reset() to release any held buffers and then freeing the memory allocated for the IndexFetchHeapData structure. This function completes the lifecycle of index fetch operations that began with heapam_index_fetch_begin(), ensuring proper resource deallocation and preventing memory leaks.

## Parameters / Member Variables
- : Pointer to IndexFetchTableData structure (cast internally to IndexFetchHeapData) representing the index fetch scan to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - [heapam_index_fetch_reset](heapam_index_fetch_reset.md) (releases any held buffers)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [IndexFetchHeapData](../I/IndexFetchHeapData.md) (heap-specific index fetch data structure)
- Called from (representative examples):
  - Part of TableAmRoutine structure as a callback function
  - Referenced by SampleHeapTupleVisible

## Notes and Other Information
- Must be called to properly terminate index fetch scans initiated by heapam_index_fetch_begin()
- Ensures both buffer cleanup (via reset) and memory deallocation (via pfree)
- Part of the complete index fetch operation lifecycle (begin, fetch, reset, end)
- Failure to call this function would result in memory leaks
- The function assumes the scan parameter was allocated by heapam_index_fetch_begin()

## Simplified Source

```c
static void heapam_index_fetch_end(IndexFetchTableData *scan) {
    IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;

    // Release any held buffers
    heapam_index_fetch_reset(scan);

    // Free the allocated scan structure
    pfree(hscan);
}
```