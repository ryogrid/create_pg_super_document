# itemIdCompactData

## Location
src/backend/storage/page/bufpage.c: 437 - 442

## Overview
A structure used to organize tuple data during page defragmentation operations in PostgreSQL heap pages.

## Definition


## Detailed Description
The `itemIdCompactData` structure serves as a temporary data organization tool during tuple defragmentation operations in PostgreSQL's buffer page management system. It is specifically designed to support the `PageRepairFragmentation` and `PageIndexMultiDelete` functions by storing essential information about each tuple that needs to be relocated during page compaction.

When a page becomes fragmented due to tuple deletions or updates, this structure helps track the current location, target position, and size requirements for each tuple that needs to be moved. The structure enables efficient sorting and reorganization of tuples to eliminate gaps and optimize page space utilization.

The structure is typically used in arrays (e.g., `itemIdCompactData itemidbase[MaxHeapTuplesPerPage]`) to process multiple tuples simultaneously during defragmentation operations.

## Parameters / Member Variables
- `offsetindex`: Zero-based index into the line pointer array, identifying which line pointer corresponds to this tuple data
- `itemoff`: Current byte offset within the page where the actual tuple data is stored
- `alignedlen`: Length of the tuple data rounded up to the next MAXALIGN boundary for proper memory alignment

## Dependencies
- Functions called/Symbols referenced: None (this is a data structure definition)
- Used by:
  - `itemIdCompact` (typedef pointer to this struct)
  - [PageRepairFragmentation](../P/PageRepairFragmentation.md) (uses arrays of this structure for heap page defragmentation)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md) (uses this structure for index page tuple deletion and compaction)

## Notes and Other Information
- This structure is part of PostgreSQL's buffer page management system located in `src/backend/storage/page/bufpage.c`
- The structure is designed to be lightweight and efficient for sorting operations during tuple compaction
- The `alignedlen` field ensures proper memory alignment requirements are maintained during tuple movement
- Arrays of this structure can be sorted by `itemoff` to determine optimal tuple movement strategies
- The structure supports both presorted (common case) and non-presorted tuple reorganization scenarios for performance optimization
- Used internally only during page maintenance operations and is not exposed to higher-level PostgreSQL components