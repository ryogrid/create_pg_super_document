# BitmapHeapPath

## Location
[src/include/nodes/pathnodes.h:1784-1788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1784-L1788)

## Overview
BitmapHeapPath represents a query execution path that uses one or more index scans to generate TID bitmaps, combines them with AND/OR operations, and then performs a heap scan using the resulting bitmap.

## Definition

```c
typedef struct BitmapHeapPath
{
	Path		path;
	Path	   *bitmapqual;		/* IndexPath, BitmapAndPath, BitmapOrPath */
} BitmapHeapPath;
```
## Detailed Description
BitmapHeapPath represents a specialized access path that generates TID (tuple identifier) bitmaps from one or more index scans, rather than directly accessing heap tuples. This approach is particularly efficient when:
1. Multiple indexes can be used for different parts of a WHERE clause
2. The selectivity of index conditions results in scattered heap access patterns
3. AND/OR combinations of index conditions are needed

The execution process involves:
1. One or more IndexPath nodes generate TID bitmaps through BitmapIndexScan operations
2. BitmapAndPath and BitmapOrPath nodes combine these bitmaps using logical operations
3. A final BitmapHeapScan reads heap tuples in physical order based on the combined bitmap

The output is always considered unordered since tuples are retrieved in physical heap order regardless of the underlying index ordering. This design provides efficient scattered I/O patterns and allows complex boolean combinations of index conditions.

## Parameters / Member Variables
- : Base Path structure containing cost estimates, row counts, and other path properties
- : Pointer to the bitmap-generating portion of the plan, which can be:
  - IndexPath (for single index bitmap generation)
  - BitmapAndPath (for AND combinations of bitmaps)
  - BitmapOrPath (for OR combinations of bitmaps)

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base path structure)
  - [IndexPath](../I/IndexPath.md) (for individual index scans)
  - [BitmapAndPath](BitmapAndPath.md) (for AND combinations)
  - [BitmapOrPath](BitmapOrPath.md) (for OR combinations)

- Called from (representative examples):
  - [create_bitmap_heap_path](../c/create_bitmap_heap_path.md) (path creation)
  - [create_index_paths](../c/create_index_paths.md) (during path generation)
  - [create_bitmap_scan_plan](../c/create_bitmap_scan_plan.md) (plan creation)
  - [bitmap_scan_cost_est](../b/bitmap_scan_cost_est.md) (cost estimation)
  - [reparameterize_path](../r/reparameterize_path.md) (path reparameterization)

## Notes and Other Information
- The same IndexPath node can represent both regular IndexScan and BitmapIndexScan usage
- [IndexPath](../I/IndexPath.md) costs always represent regular/index-only scan costs; BitmapIndexScan costs are computed separately using indextotalcost and indexselectivity
- Output ordering is always considered lost due to physical heap order retrieval
- Particularly effective for queries with complex WHERE clauses involving multiple indexes
- The bitmap approach reduces random I/O by clustering heap accesses