# create_bitmap_subplan

## Location
[src/backend/optimizer/plan/createplan.c:3332-3539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3332-L3539)

## Overview
Recursively converts a bitmap qualification tree (BitmapAndPath, BitmapOrPath, or IndexPath) into executable Plan nodes while extracting qualification information for bitmap scan optimization.

## Definition
```c
static Plan *
create_bitmap_subplan(PlannerInfo *root, Path *bitmapqual,
                      List **qual, List **indexqual, List **indexECs)
```

## Detailed Description
The `create_bitmap_subplan` function is a recursive tree-processing function that converts bitmap qualification paths into executable plan nodes. It handles three types of bitmap path nodes:

1. **BitmapAndPath**: Creates a `BitmapAnd` plan that performs logical AND operations on multiple bitmap results. Uses `list_concat_unique` to eliminate obvious duplicates among subplan qualifications.

2. **BitmapOrPath**: Creates a `BitmapOr` plan that performs logical OR operations. Optimizes for qual-free subplans (reducing \... OR true\ to just \true\) and avoids expensive duplicate elimination due to potentially large OR lists from IN clauses.

3. **IndexPath**: Converts regular index paths to `BitmapIndexScan` nodes by first creating a temporary `IndexScan` via `create_indexscan_plan`, then extracting the necessary components for bitmap operation.

The function returns multiple outputs:
- **qual**: Original index conditions (for rechecking if bitmap becomes lossy)
- **indexqual**: Actual indexable conditions derived from special operators
- **indexECs**: EquivalenceClass pointers for redundancy detection

This function is essential for bitmap scan optimization, enabling PostgreSQL to efficiently handle complex multi-index queries and boolean expressions.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context information
- `bitmapqual`: Path node representing the bitmap qualification tree (BitmapAndPath, BitmapOrPath, or IndexPath)
- `qual`: Output parameter returning list of original index conditions for potential rechecking
- `indexqual`: Output parameter returning list of actual indexable conditions
- `indexECs`: Output parameter returning list of EquivalenceClass pointers for top-level indexquals

## Dependencies
- Functions called/Symbols referenced:
  - [list_concat_unique](../l/list_concat_unique.md)
  - [list_concat](../l/list_concat.md)
  - [make_bitmap_and](../m/make_bitmap_and.md)
  - [make_bitmap_or](../m/make_bitmap_or.md)
  - [make_bitmap_indexscan](../m/make_bitmap_indexscan.md)
  - [make_ands_explicit](../m/make_ands_explicit.md)
  - [make_orclause](../m/make_orclause.md)
  - [create_indexscan_plan](create_indexscan_plan.md)
  - [clamp_row_est](clamp_row_est.md)
  - [get_actual_clauses](../g/get_actual_clauses.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - nodeTag
  - [BitmapAndPath](../B/BitmapAndPath.md), BitmapOrPath, IndexPath, IndexScan (struct types)
  - [IndexClause](../I/IndexClause.md) (struct type)
- Called from (representative examples):
  - [create_bitmap_scan_plan](create_bitmap_scan_plan.md)
  - [create_bitmap_subplan](create_bitmap_subplan.md) (recursive calls)

## Notes and Other Information
- This function is recursive and can handle arbitrarily complex bitmap qualification trees
- Includes optimization for ScalarArrayOpExpr quals that may result in single-subpath BitmapOrPaths
- Handles partial index predicates by checking for redundancy before including them in qualifications
- Uses different duplicate elimination strategies for AND vs OR operations based on performance characteristics
- The function uses `clamp_row_est` to ensure row estimates remain within reasonable bounds
- Essential for multi-index bitmap scan optimization, particularly effective for complex WHERE clauses with multiple indexed conditions
- Supports proper cost estimation by preserving startup and total costs from the original paths
- Returns qual information in forms suitable for both bitmap generation and potential lossy rechecking