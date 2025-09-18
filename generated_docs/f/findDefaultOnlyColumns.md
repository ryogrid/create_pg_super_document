# findDefaultOnlyColumns

## Location
[src/backend/rewrite/rewriteHandler.c:1315-1402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1315-L1402)

## Overview
Searches a VALUES range table entry to identify columns that contain only SetToDefault items across all rows, returning a bitmapset of such column attribute numbers.

## Definition
```c
static Bitmapset *findDefaultOnlyColumns(RangeTblEntry *rte)
```

## Detailed Description
This function analyzes a VALUES range table entry to determine which columns contain exclusively SetToDefault nodes across all value rows. This information is crucial for optimization purposes in the query rewriter, as columns that are entirely default can be handled more efficiently.

The algorithm works in two phases:
1. **Initialization**: Processes the first row to build an initial bitmap of columns containing SetToDefault
2. **Refinement**: For each subsequent row, removes columns from the bitmap if they contain non-default values

The function uses early termination - if at any point no columns remain that are default-only across all processed rows, it stops processing and returns an empty result.

This analysis enables the rewriter to optimize queries like `INSERT INTO table VALUES (1, DEFAULT, 3), (2, DEFAULT, 4)` by recognizing that the second column is consistently default across all rows.

## Parameters / Member Variables
- `rte`: The RangeTblEntry representing a VALUES clause to analyze

## Dependencies
- Functions called/Symbols referenced:
  - [SetToDefault](../S/SetToDefault.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_del_member](../b/bms_del_member.md)
  - bms_is_empty
- Called from (representative examples):
  - [rewriteTargetListIU](../r/rewriteTargetListIU.md)

## Notes and Other Information
- Returns NULL if no columns are default-only across all rows
- Uses bitmapset operations for efficient column tracking
- Employs early termination optimization when no default-only columns remain
- Column numbering is 1-based (attribute numbers)
- Critical for VALUES clause optimization in INSERT/UPDATE operations
- Helps distinguish between mixed-value columns and pure-default columns