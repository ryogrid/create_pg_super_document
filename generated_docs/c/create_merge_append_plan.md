# create_merge_append_plan

## Location
[src/backend/optimizer/plan/createplan.c:1438-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L1438-L1587)

## Overview
Creates a MergeAppend plan node that merges multiple sorted child plans into a single sorted output stream, commonly used for partitioned table queries where results need to be returned in sorted order.

## Definition

```c
static Plan *
create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path,
						 int flags)
```
## Detailed Description
The  function creates a MergeAppend execution plan node from a MergeAppendPath. This plan type is used when the optimizer needs to combine results from multiple child plans (typically from different partitions of a partitioned table) while maintaining a specific sort order. The function ensures that all child plans produce output in the same sort order by potentially adding Sort nodes where necessary.

The function performs several key operations:
1. Creates the MergeAppend node structure and copies generic path information
2. Computes sort column information using  
3. Recursively creates child plans, ensuring they all return compatible target lists
4. Validates that all children have matching sort key information
5. Adds explicit Sort nodes to children that aren't already properly sorted
6. Sets up partition pruning information if enabled and applicable
7. Optionally injects a projection node if the target list was modified during sort preparation

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : MergeAppendPath representing the chosen path with multiple sorted subpaths to merge
- : Control flags (CP_EXACT_TLIST, CP_SMALL_TLIST, etc.) that affect target list handling

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create MergeAppend node)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [make_sort](../m/make_sort.md)
  - [label_sort_with_costsize](../l/label_sort_with_costsize.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [make_partition_pruneinfo](../m/make_partition_pruneinfo.md)
  - [inject_projection_plan](../i/inject_projection_plan.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md) (main recursive plan creation function)

## Notes and Other Information
- The function assumes all child paths have compatible sort orders that can be merged
- Explicit Sort nodes are only added to children that don't already satisfy the required sort order
- Partition pruning information is collected when  is true and base restriction clauses exist
- The function handles target list adjustments carefully, potentially injecting a projection node if sort columns were added but exact/small target list was requested
- Currently does not support parameterized MergeAppend paths (asserted in the code)
- Used primarily for queries on partitioned tables where sorted results are needed