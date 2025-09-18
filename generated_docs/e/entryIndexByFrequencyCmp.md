# entryIndexByFrequencyCmp

## Location
src/backend/access/gin/ginget.c: 488 - 504

## Overview
This is a comparison function used for sorting scan entry indexes by their predicted result frequency, prioritizing least frequent items first to optimize GIN index scan performance.

## Definition
```c
static int entryIndexByFrequencyCmp(const void *a1, const void *a2, void *arg)
```

## Detailed Description
The function implements a comparison callback suitable for use with sorting algorithms (like qsort_r). It compares two scan entry indexes based on their `predictNumberResult` values, which represent the estimated number of matching tuples for each scan entry. By sorting entries in ascending order of frequency (least frequent first), the GIN index scan can optimize query execution by processing the most selective conditions first, potentially reducing the overall work needed for bitmap operations.

This is a classic query optimization technique where more selective (less frequent) conditions are evaluated first to minimize the size of intermediate results in subsequent operations.

## Parameters / Member Variables
- `a1`: Pointer to the first scan entry index (as `const int *`)
- `a2`: Pointer to the second scan entry index (as `const int *`)
- `arg`: Pointer to GinScanKey structure containing the array of scan entries

## Dependencies
- Functions called/Symbols referenced:
  - GinScanKey (structure type for accessing scan entries array)
- Called from:
  - startScanKey (src/backend/access/gin/ginget.c:557)

## Notes and Other Information
- This is a static function, only accessible within the ginget.c file  
- Returns -1 if first entry is less frequent, 0 if equal frequency, 1 if first entry is more frequent
- Used as a callback function for sorting algorithms (typically qsort_r)
- Part of GIN index query optimization strategy that prioritizes selective conditions
- Compares `predictNumberResult` values which are populated during scan entry initialization
- The sorting enables efficient execution order for multiple scan conditions in complex queries
- Essential for minimizing computational cost in multi-key GIN index scans