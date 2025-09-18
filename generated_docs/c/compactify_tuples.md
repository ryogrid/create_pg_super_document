# compactify_tuples

## Location
src/backend/storage/page/bufpage.c: 474 - 698

## Overview
Removes gaps in a page by moving tuples to eliminate fragmentation and reordering them into reverse line pointer order for optimal performance.

## Definition
static void compactify_tuples(itemIdCompact itemidbase, int nitems, Page page, bool presorted)

## Detailed Description
This static function performs tuple compactification to eliminate gaps caused by removed or unused line pointers. It has two optimized code paths depending on whether the input array is presorted. When presorted (tuples in descending order by offset), it uses efficient memmove operations. For non-presorted data, it uses a temporary buffer approach to avoid overwriting tuples during the move operations. The function reorders tuples back into reverse line pointer order, which increases the likelihood of hitting the optimal presorted case in future operations. This is a performance-critical function that includes several optimizations to minimize memory operations.

## Parameters / Member Variables
- itemidbase: Array of itemIdCompact structures representing tuples to be compacted
- nitems: Number of items in the itemidbase array (must be > 0)
- page: The page containing the tuples to be compacted
- presorted: Boolean indicating if itemidbase is sorted in descending order of itemoff

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - memmove
  - memcpy
  - Assert (for debugging)
- Data types used:
  - itemIdCompact
  - PageHeader
  - Offset
  - ItemId
  - PGAlignedBlock
- Called from:
  - [PageRepairFragmentation](../P/PageRepairFragmentation.md) (main page defragmentation function)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md) (bulk tuple deletion operations)

## Notes and Other Information
- This is a static function internal to bufpage.c, not exposed in the public API
- The function includes two distinct algorithms: one for presorted input (using memmove) and one for non-presorted input (using temporary buffer)
- When less than 25% of tuples remain (>75% pruned), it uses a tuple-by-tuple copy approach for better efficiency
- The function includes extensive assertions in debug builds to verify the presorted parameter
- Performance-optimized to minimize memory operations and take advantage of common access patterns
- Updates the page header's pd_upper field to reflect the new free space boundary
- The function is located in src/backend/storage/page/bufpage.c:474-698