# ginCompareAttEntries

## Location
[src/backend/access/gin/ginutil.c:410-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L410-L432)

## Overview
Compares two GIN index keys that may belong to different columns, first comparing by attribute number then delegating to ginCompareEntries for same-column comparisons.

## Definition
```c
int ginCompareAttEntries(GinState *ginstate,
                        OffsetNumber attnuma, Datum a, GinNullCategory categorya,
                        OffsetNumber attnumb, Datum b, GinNullCategory categoryb)
```

## Detailed Description
ginCompareAttEntries extends the comparison capability of ginCompareEntries to handle keys from different columns within the same GIN index. This function implements a two-level sorting hierarchy: it first compares entries by their attribute (column) number, ensuring that all entries for column 1 come before column 2, etc. When the attribute numbers are equal, it delegates to ginCompareEntries for the actual value comparison.

This function is essential for multi-column GIN indexes where entries from different columns need to be properly ordered within the same index structure. It maintains the invariant that entries are first sorted by column, then by value within each column, which is crucial for efficient index operations and page organization.

## Parameters / Member Variables
- `ginstate`: Pointer to GIN state structure containing comparison functions and collation information
- `attnuma`: 1-based attribute number for the first entry
- `a`: First datum value to compare
- `categorya`: Null category classification for the first value
- `attnumb`: 1-based attribute number for the second entry  
- `b`: Second datum value to compare
- `categoryb`: Null category classification for the second value

## Dependencies
- Functions called/Symbols referenced:
  - [ginCompareEntries](ginCompareEntries.md): Core comparison function for same-column entries
  - [GinState](../G/GinState.md): Structure containing GIN index state and comparison functions
  - GinNullCategory: Enumeration for categorizing null and special values

- Called from (representative examples):
  - [cmpEntryAccumulator](../c/cmpEntryAccumulator.md): During bulk loading operations for sorting entries
  - [entryIsMoveRight](../e/entryIsMoveRight.md): When determining page navigation in entry pages
  - [entryLocateEntry](../e/entryLocateEntry.md): During binary search operations in entry pages
  - [entryLocateLeafEntry](../e/entryLocateLeafEntry.md): When locating entries in leaf pages

## Notes and Other Information
- Returns negative, zero, or positive integer following standard comparison conventions
- Attribute number comparison takes absolute precedence over value comparison
- Essential for maintaining proper sort order in multi-column GIN indexes
- Enables efficient binary search operations across entry pages containing multiple columns
- Used primarily in entry page operations where cross-column comparisons are needed
- Ensures consistent ordering that supports both single-column and multi-column GIN index operations
- The function assumes 1-based attribute numbering consistent with PostgreSQL conventions