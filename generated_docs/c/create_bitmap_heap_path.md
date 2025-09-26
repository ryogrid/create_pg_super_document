# create_bitmap_heap_path

## Location
[src/backend/optimizer/util/pathnode.c:1042-1074](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1042-L1074)

## Overview
Creates a path node for a bitmap heap scan, which represents an execution plan that first uses bitmap index scan(s) to collect a bitmap of matching tuple IDs and then performs a heap scan guided by that bitmap.

## Definition

```c
union of what the child paths
	 * depend on.  (Alternatively, we could insist that the caller pass this
	 * in, but it's more convenient and reliable to compute it here.)
	 */
	foreach(lc, bitmapquals)
	{
		Path	   *bitmapqual = (Path *) lfirst(lc);

		required_outer = bms_add_members(required_outer,
										 PATH_REQ_OUTER(bitmapqual));
	}
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
```
## Detailed Description
This function constructs a BitmapHeapPath node that represents a bitmap heap scan access path. A bitmap heap scan is a two-phase operation: first, one or more index scans create a bitmap indicating which heap pages contain matching tuples, then the heap is scanned in physical page order using this bitmap to guide the scan. This approach is particularly efficient when multiple indexes can be combined or when the selectivity results in scattered tuple locations that would make individual index lookups inefficient.

The function initializes all the standard Path fields and calls cost_bitmap_heap_scan to estimate the execution costs. The resulting path is always considered unordered (pathkeys = NIL) since the heap scan follows physical page order rather than any logical ordering.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and optimization information
- : RelOptInfo for the relation being scanned, containing statistics and metadata
- : Tree of IndexPath, BitmapAndPath, and BitmapOrPath nodes representing the bitmap qualification
- : Set of outer relation IDs needed for a parameterized path (for joins)
- : Number of repetitions of the indexscan to factor into caching behavior estimates
- : Degree of parallelism (0 for non-parallel execution)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new BitmapHeapPath node)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md) (gets parameterization info)
  - [cost_bitmap_heap_scan](cost_bitmap_heap_scan.md) (calculates execution costs)
- Called from (representative examples):
  - [create_partial_bitmap_paths](create_partial_bitmap_paths.md) (for parallel bitmap scans)
  - [create_index_paths](create_index_paths.md) (when considering bitmap scan alternatives)
  - [reparameterize_path](../r/reparameterize_path.md) (when adjusting path parameters)

## Notes and Other Information
- The resulting path is always unordered since bitmap heap scans follow physical page order
- The loop_count parameter should match the value used when creating component IndexPaths for consistent cost estimation
- Supports parallel execution when parallel_degree > 0
- The bitmapqual can be a complex tree structure combining multiple indexes through AND/OR operations
- Cost estimation considers both the bitmap creation phase and the heap scanning phase