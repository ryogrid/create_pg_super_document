# BitmapOrPath

## Location
[src/include/nodes/pathnodes.h:1809-1814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1809-L1814)

## Overview
BitmapOrPath represents a BitmapOr plan node that performs logical OR operations on multiple TID bitmaps as part of a BitmapHeapPath execution plan.

## Definition

```c
typedef struct BitmapOrPath
{
	Path		path;
	List	   *bitmapquals;	/* IndexPaths and BitmapAndPaths */
	Selectivity bitmapselectivity;
} BitmapOrPath;
```
## Detailed Description
BitmapOrPath represents a logical OR operation between multiple bitmap-generating paths in PostgreSQL's bitmap scan execution strategy. It can only appear as part of the substructure of a BitmapHeapPath and serves to combine multiple TID bitmaps using union logic.

The node takes multiple bitmap-generating paths (IndexPaths or BitmapAndPaths) and produces a single bitmap containing TIDs that appear in ANY of the input bitmaps. This is particularly useful for queries with multiple WHERE conditions connected by OR operators, where different conditions can be satisfied by different indexes - the OR operation ensures tuples meeting any of the conditions are included in the final result.

Like BitmapAndPath, the Path structure provides consistency with other path types, though it's somewhat heavyweight for this specific use case. The design maintains simplicity and uniformity across the planner's path representation.

## Parameters / Member Variables
- : Base Path structure containing cost estimates, row counts, and path properties for the OR operation
- : List of child paths that generate bitmaps to be ORed together, containing:
  - IndexPaths (for individual index bitmap generation)
  - BitmapAndPaths (for AND-combined bitmaps)
- : Combined selectivity estimate for the OR operation, representing the fraction of pages expected to contain qualifying tuples

## Dependencies
- Functions called/Symbols referenced:
  - Path (base path structure)
  - List (PostgreSQL list structure)  
  - Selectivity (selectivity estimation type)
  - IndexPath (for individual index conditions)
  - BitmapAndPath (for AND-combined conditions)

- Called from (representative examples):
  - create_bitmap_or_path (path creation)
  - cost_bitmap_or_node (cost estimation)
  - create_bitmap_subplan (plan generation)
  - find_indexpath_quals (path finding)
  - create_bitmap_and_path (as child of AND operations)

## Notes and Other Information
- Can only appear as part of a BitmapHeapPath substructure, not as a standalone path
- The OR operation increases the number of heap pages that need to be scanned compared to individual conditions
- Selectivity calculation considers the union of all child conditions, accounting for overlap
- Particularly effective for queries like "WHERE indexed_col1 = ? OR indexed_col2 = ?"
- Can be nested within BitmapAndPath nodes to create complex boolean expressions
- The Path structure is somewhat heavier than needed but maintains consistency
- Child paths can include both IndexPaths and BitmapAndPaths for complex nested boolean logic