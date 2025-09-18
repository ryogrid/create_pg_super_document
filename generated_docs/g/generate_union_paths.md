# generate_union_paths

## Location
src/backend/optimizer/prep/prepunion.c: 696 - 1017

## Overview
Generates and evaluates multiple execution paths for UNION and UNION ALL operations, creating an optimized RelOptInfo with various path strategies including sorted, hashed, and parallel approaches.

## Definition


## Detailed Description
This function is the core path generation engine for UNION operations in PostgreSQL's query planner. It orchestrates the creation of multiple execution strategies for combining results from union children:

1. **Child Planning**: First calls  to recursively handle nested UNION operations and build the list of child relations
2. **Target List Generation**: Creates the target list for the Append/MergeAppend plan node using 
3. **Path Strategy Selection**: For UNION (not UNION ALL), determines whether sorting or hashing strategies are viable
4. **Multiple Path Creation**: Generates various execution paths:
   - Append path using cheapest paths from each child
   - Parallel Gather+Append path when partial paths are available
   - Hash aggregate path for deduplication (UNION only)
   - Sort+Unique path for deduplication (UNION only)  
   - MergeAppend+Unique path when sorted paths are available (UNION only)

The function handles both UNION and UNION ALL semantics, with UNION requiring deduplication through either hashing or sorting strategies.

## Parameters / Member Variables
- : SetOperationStmt containing the UNION operation details including the 'all' flag and column types
- : PlannerInfo containing global planning context and configuration
- : List of reference names for the target list columns
- : Output parameter returning the generated target list for the operation

## Dependencies
- Functions called/Symbols referenced:
  - plan_union_children
  - generate_append_tlist  
  - generate_setop_grouplist
  - grouping_is_sortable
  - grouping_is_hashable
  - make_pathkeys_for_sortclauses
  - build_setop_child_paths
  - create_append_path
  - create_gather_path
  - create_agg_path
  - create_sort_path
  - create_upper_unique_path
  - create_merge_append_path
- Called from (representative examples):
  - recurse_set_operations

## Notes and Other Information
- For UNION operations, the function assumes worst-case estimates for the number of distinct groups (equal to total input size)
- Parallel execution is considered when all child relations support parallelism and have partial paths
- The function automatically merges identical nested UNION nodes to optimize the plan structure
- When type coercion is required due to mismatching types among union children, sorted paths may become unavailable
- The choice between hash and sort-based deduplication depends on the grouping characteristics of the operation