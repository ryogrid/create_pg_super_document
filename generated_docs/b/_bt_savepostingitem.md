# _bt_savepostingitem

## Location
src/backend/access/nbtree/nbtsearch.c: 2013 - 2040

## Overview
Saves subsequent TIDs from a posting list tuple into the scan position's item array, after initial setup by _bt_setuppostingitems().

## Definition
```c
static inline void _bt_savepostingitem(BTScanOpaque so, int itemIndex, OffsetNumber offnum, ItemPointer heapTid, int tupleOffset)
```

## Detailed Description
This inline function is used to save additional heap TIDs from a posting list tuple during B-tree scanning. It works in conjunction with _bt_setuppostingitems(), which must be called first to set up the base tuple. This function saves each subsequent TID from the posting list, reusing the same base tuple offset for index-only scans to optimize memory usage and ensure consistency.

## Parameters / Member Variables
- `so`: B-tree scan opaque structure containing the current scan state
- `itemIndex`: Index position in the items array where this TID should be stored
- `offnum`: Offset number of the posting list tuple on the current page
- `heapTid`: Pointer to the heap TID to be saved
- `tupleOffset`: Tuple storage offset returned by _bt_setuppostingitems() for the base tuple

## Dependencies
- Functions called/Symbols referenced:
  - BTScanOpaque (scan state structure)
  - [BTScanPosItem](../B/BTScanPosItem.md) (item storage structure)
- Called from (representative examples):
  - [_bt_readpage](_bt_readpage.md) (during posting list tuple processing)

## Notes and Other Information
- Must be preceded by a call to _bt_setuppostingitems() for the same posting list tuple
- Designed for index-only scans where all TIDs from the same posting list share the same base tuple data
- The tupleOffset parameter ensures all items from the same posting list reference the same base tuple in memory
- Declared as inline for performance optimization since it's called frequently during posting list processing
- Part of PostgreSQL's posting list feature that reduces index size by storing multiple heap TIDs per index entry