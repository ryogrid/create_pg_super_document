# create_partial_distinct_paths

## Location
[src/backend/optimizer/plan/planner.c:4900-5098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L4900-L5098)

## Overview
Creates partial distinct paths for parallel execution by processing input relation's partial paths and adding unique/aggregate paths to the UPPERREL_PARTIAL_DISTINCT relation, with Gather/GatherMerge paths on top to remove duplicates from parallel workers.

## Definition

```c
static void
create_partial_distinct_paths(PlannerInfo *root, RelOptInfo *input_rel,
							  RelOptInfo *final_distinct_rel,
							  PathTarget *target)
```
## Detailed Description
This function is responsible for creating partial execution paths for DISTINCT operations in parallel query execution. It processes the input relation's partial paths and generates appropriate paths for the partial distinct phase of query execution. The function handles both sort-based and hash-based approaches to eliminate duplicates within each parallel worker, then creates Gather paths to combine results from multiple workers. The final step involves calling create_final_distinct_paths to handle any remaining duplicates that may arise from combining parallel worker results.

The function implements several optimization strategies:
- Uses incremental sorting when paths are partially sorted
- Applies limit paths when all tuples have the same distinct value
- Creates hash aggregate paths when hashing is possible and enabled
- Integrates with FDW (Foreign Data Wrapper) systems for distributed query processing
- Supports extension hooks for custom path generation

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and configuration
- : RelOptInfo for the input relation containing partial paths to process  
- : RelOptInfo for the final distinct relation where complete paths will be stored
- : PathTarget specifying the target list and sorting requirements for the distinct operation

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [get_sortgrouplist_exprs](../g/get_sortgrouplist_exprs.md)
  - [estimate_num_groups](../e/estimate_num_groups.md)
  - [grouping_is_sortable](../g/grouping_is_sortable.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](create_sort_path.md)
  - [create_incremental_sort_path](create_incremental_sort_path.md)
  - [create_limit_path](create_limit_path.md)
  - [create_upper_unique_path](create_upper_unique_path.md)
  - [create_agg_path](create_agg_path.md)
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md)
  - [create_final_distinct_paths](create_final_distinct_paths.md)
- Called from:
  - [create_distinct_paths](create_distinct_paths.md)

## Notes and Other Information
- Early returns if input relation has no partial paths or uses DISTINCT ON (which cannot be parallelized)
- Handles the special case where distinct_pathkeys is NIL by applying limit paths to restrict each worker to 1 tuple
- Respects enable_hashagg and enable_incremental_sort configuration parameters
- Preserves FDW relationship information from input to partial distinct relation
- Uses AGGSPLIT_SIMPLE for hash aggregation in the partial phase
- The function is part of the upper-level query planning infrastructure for parallel DISTINCT operations