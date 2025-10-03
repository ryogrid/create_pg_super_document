# SampleHeapTupleVisible

## Location
[src/backend/access/heap/heapam_handler.c:2543-2652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2543-L2652)

## Overview
SampleHeapTupleVisible determines whether a tuple is visible during table sampling operations, optimizing visibility checks based on the scan mode.

## Definition
```c
static bool
SampleHeapTupleVisible(TableScanDesc scan, Buffer buffer,
                      HeapTuple tuple,
                      OffsetNumber tupoffset)
```

## Detailed Description
This function provides optimized visibility checking for tuples during table sampling operations. It implements two different strategies based on the scan mode:

1. **Page-at-a-time mode (SO_ALLOW_PAGEMODE)**: When enabled, the function relies on pre-computed visibility information stored in the rs_vistuples array by heap_prepare_pagescan(). It uses a binary search over this sorted array to quickly determine if a tuple at the given offset is visible, avoiding redundant visibility checks.

2. **Individual tuple mode**: When page-at-a-time mode is not enabled, it falls back to checking each tuple individually using HeapTupleSatisfiesVisibility().

The binary search optimization is particularly valuable for sampling operations where tuples may not be selected in sequential order, providing efficient O(log n) lookup time for visibility determination.

## Parameters / Member Variables
- `scan`: TableScanDesc containing the scan context and configuration flags
- `buffer`: Buffer containing the page where the tuple resides
- `tuple`: HeapTuple to check for visibility
- `tupoffset`: OffsetNumber indicating the position of the tuple within the page

## Dependencies
- Functions called/Symbols referenced:
  - [HeapScanDesc](../H/HeapScanDesc.md) (cast from TableScanDesc)
  - SO_ALLOW_PAGEMODE (scan flag)
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md)
- Called from (representative examples):
  - [heapam_scan_sample_next_tuple](../h/heapam_scan_sample_next_tuple.md)

## Notes and Other Information
- This is a static function internal to heapam_handler.c, specifically optimized for sampling operations
- The binary search implementation assumes rs_vistuples array is sorted by offset number
- Page-at-a-time mode significantly improves performance by avoiding repeated visibility checks for the same page
- The function is part of the heap access method's table sampling infrastructure
- Returns true if the tuple is visible according to the scan's snapshot, false otherwise

## Simplified Source

```c
static bool
SampleHeapTupleVisible(TableScanDesc scan, Buffer buffer,
                       HeapTuple tuple, OffsetNumber tupoffset)
{
    HeapScanDesc hscan = (HeapScanDesc) scan;

    if (scan->rs_flags & SO_ALLOW_PAGEMODE) {
        // In pagemode, visibility was pre-computed by heap_prepare_pagescan
        // Use binary search to find tuple in the sorted rs_vistuples array
        int start = 0, end = hscan->rs_ntuples - 1;

        while (start <= end) {
            int mid = (start + end) / 2;
            OffsetNumber curoffset = hscan->rs_vistuples[mid];

            if (tupoffset == curoffset)
                return true;
            else if (tupoffset < curoffset)
                end = mid - 1;
            else
                start = mid + 1;
        }

        return false;
    }
    else {
        // Check tuple visibility individually
        return HeapTupleSatisfiesVisibility(tuple, scan->rs_snapshot, buffer);
    }
}
```