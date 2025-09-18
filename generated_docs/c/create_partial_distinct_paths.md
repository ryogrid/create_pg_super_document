# create_partial_distinct_paths

## Location
src/backend/optimizer/plan/planner.c: 4900 - 5098

## Overview
Creates partial distinct paths for parallel execution by processing input relation's partial paths and adding unique/aggregate paths to the UPPERREL_PARTIAL_DISTINCT relation, with Gather/GatherMerge paths on top to remove duplicates from parallel workers.

## Definition


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
  - fetch_upper_rel
  - get_sortgrouplist_exprs
  - estimate_num_groups
  - grouping_is_sortable
  - pathkeys_count_contained_in
  - create_sort_path
  - create_incremental_sort_path
  - create_limit_path
  - create_upper_unique_path
  - create_agg_path
  - generate_useful_gather_paths
  - create_final_distinct_paths
- Called from:
  - create_distinct_paths

## Notes and Other Information
- Early returns if input relation has no partial paths or uses DISTINCT ON (which cannot be parallelized)
- Handles the special case where distinct_pathkeys is NIL by applying limit paths to restrict each worker to 1 tuple
- Respects enable_hashagg and enable_incremental_sort configuration parameters
- Preserves FDW relationship information from input to partial distinct relation
- Uses AGGSPLIT_SIMPLE for hash aggregation in the partial phase
- The function is part of the upper-level query planning infrastructure for parallel DISTINCT operations