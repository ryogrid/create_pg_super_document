# create_merge_append_plan

## Location
src/backend/optimizer/plan/createplan.c: 1438 - 1587

## Overview
Creates a MergeAppend plan node that merges multiple sorted child plans into a single sorted output stream, commonly used for partitioned table queries where results need to be returned in sorted order.

## Definition


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
  - build_path_tlist
  - copy_generic_path_info
  - prepare_sort_from_pathkeys
  - create_plan_recurse
  - pathkeys_contained_in
  - make_sort
  - label_sort_with_costsize
  - extract_actual_clauses
  - make_partition_pruneinfo
  - inject_projection_plan
- Called from (representative examples):
  - create_plan_recurse (main recursive plan creation function)

## Notes and Other Information
- The function assumes all child paths have compatible sort orders that can be merged
- Explicit Sort nodes are only added to children that don't already satisfy the required sort order
- Partition pruning information is collected when  is true and base restriction clauses exist
- The function handles target list adjustments carefully, potentially injecting a projection node if sort columns were added but exact/small target list was requested
- Currently does not support parameterized MergeAppend paths (asserted in the code)
- Used primarily for queries on partitioned tables where sorted results are needed