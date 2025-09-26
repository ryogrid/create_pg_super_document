# BitmapAndPath

## Location
src/include/nodes/pathnodes.h: 1796 - 1801

## Overview
BitmapAndPath represents a BitmapAnd plan node that performs logical AND operations on multiple TID bitmaps as part of a BitmapHeapPath execution plan.

## Definition

```c
typedef struct BitmapAndPath
{
	Path		path;
	List	   *bitmapquals;	/* IndexPaths and BitmapOrPaths */
	Selectivity bitmapselectivity;
} BitmapAndPath;
```
## Detailed Description
BitmapAndPath represents a logical AND operation between multiple bitmap-generating paths in PostgreSQL's bitmap scan execution strategy. It can only appear as part of the substructure of a BitmapHeapPath and serves to combine multiple TID bitmaps using intersection logic.

The node takes multiple bitmap-generating paths (IndexPaths or BitmapOrPaths) and produces a single bitmap containing only the TIDs that appear in ALL input bitmaps. This is particularly useful for queries with multiple WHERE conditions that can each be satisfied by different indexes - the AND operation ensures only tuples meeting all conditions are included in the final result.

The Path structure provides a consistent interface, though it's somewhat heavyweight for this specific use case. The design choice maintains simplicity and consistency with other path node types in the PostgreSQL planner.

## Parameters / Member Variables
- : Base Path structure containing cost estimates, row counts, and path properties for the AND operation
- : List of child paths that generate bitmaps to be ANDed together, containing:
  - IndexPaths (for individual index bitmap generation)
  - BitmapOrPaths (for OR-combined bitmaps)
- : Combined selectivity estimate for the AND operation, representing the fraction of pages expected to contain qualifying tuples

## Dependencies
- Functions called/Symbols referenced:
  - Path (base path structure)
  - List (PostgreSQL list structure)
  - Selectivity (selectivity estimation type)
  - IndexPath (for individual index conditions)
  - BitmapOrPath (for OR-combined conditions)

- Called from (representative examples):
  - create_bitmap_and_path (path creation)
  - cost_bitmap_and_node (cost estimation)
  - create_bitmap_subplan (plan generation)
  - bitmap_and_cost_est (cost estimation)
  - find_indexpath_quals (path finding)

## Notes and Other Information
- Can only appear as part of a BitmapHeapPath substructure, not as a standalone path
- The AND operation reduces the number of heap pages that need to be scanned
- Selectivity calculation considers the intersection of all child conditions
- Particularly effective for queries like "WHERE indexed_col1 = ? AND indexed_col2 = ?"
- The Path structure is somewhat heavier than needed but maintains consistency
- Child paths can include both IndexPaths and BitmapOrPaths for complex boolean expressions