# _bt_pgaddtup

## Location
src/backend/access/nbtree/nbtinsert.c: 2630 - 2682

## Overview
Adds a data item to a particular page during B-tree split operations, with special handling for the first data item on internal pages that requires key truncation to minus infinity.

## Definition
```c
static inline bool _bt_pgaddtup(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off, bool newfirstdataitem)
```

## Detailed Description
The `_bt_pgaddtup` function is a specialized wrapper around `PageAddItem` designed specifically for B-tree page splitting operations. Its primary purpose is to handle the special case where the first data item on an internal B-tree page needs to have its key treated as "minus infinity" after a split.

When splitting an internal page, the first item on the new right page must have all its key attributes truncated away, leaving only the downlink pointer. This ensures proper B-tree ordering where the first item represents the minimum possible key value for that subtree. This truncation is essential for maintaining B-tree invariants and proper key distribution.

The function creates a truncated copy of the index tuple when needed, setting the tuple size to contain only the essential IndexTupleData header while preserving the downlink information.

## Parameters / Member Variables
- `page`: The target page where the item will be added
- `itemsize`: Size of the item to be added
- `itup`: The index tuple to add to the page  
- `itup_off`: Offset number where the item should be placed
- `newfirstdataitem`: Boolean flag indicating if this is the first data item on a new internal page

## Dependencies
- Functions called/Symbols referenced:
  - `BTreeTupleSetNAtts`: Sets the number of attributes in the tuple to 0
  - `PageAddItem`: Adds the item to the specified page
  - `IndexTupleData`: Tuple structure for index entries
- Called from (representative examples):
  - `_bt_split`: Multiple calls during page splitting operations

## Notes and Other Information
- Only truncates tuples when `newfirstdataitem` is true for internal page splits
- The truncation creates a "minus infinity" key while preserving the downlink pointer
- Returns false if `PageAddItem` fails due to insufficient space
- This is different from suffix truncation - it's specifically for split operations
- The left page in a split doesn't need this treatment as it was already truncated in previous splits
- Marked as inline for performance in the critical split path