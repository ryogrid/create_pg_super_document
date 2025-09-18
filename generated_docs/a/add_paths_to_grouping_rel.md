# add_paths_to_grouping_rel

## Location
src/backend/optimizer/plan/planner.c: 7044 - 7278

## Overview
Creates non-partial paths for grouping operations by generating various aggregation and grouping paths from input relations, supporting both sort-based and hash-based strategies for GROUP BY, aggregation, and GROUPING SETS operations.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's query planning system that generates execution paths for grouping operations. It creates various types of paths (AggPath, GroupPath) to handle different grouping scenarios:

1. **Sort-based grouping**: Iterates through input relation paths, generates useful grouping key orderings, and creates sorted aggregation/grouping paths
2. **Partial aggregation finalization**: Processes partially aggregated paths from parallel workers by creating finalization paths
3. **Hash-based grouping**: Generates hash aggregation paths for unsorted input data
4. **GROUPING SETS support**: Handles complex grouping sets operations through specialized path creation

The function intelligently chooses between different aggregation strategies (AGG_PLAIN, AGG_SORTED, AGG_HASHED) and splitting modes (AGGSPLIT_SIMPLE, AGGSPLIT_FINAL_DESERIAL) based on the query structure and available input paths.

## Parameters / Member Variables
- : PlannerInfo containing query planning context and metadata
- : RelOptInfo representing the input relation to be grouped
- : RelOptInfo representing the target grouped relation to receive paths
- : RelOptInfo for partially aggregated results from parallel processing (can be NULL)
- : AggClauseCosts containing cost estimates for aggregate functions
- : grouping_sets_data containing information about grouping sets operations
- : Estimated number of output groups from the grouping operation
- : GroupPathExtraData containing additional flags and costs for path generation

## Dependencies
- Functions called/Symbols referenced:
  - get_useful_group_keys_orderings
  - make_ordered_path
  - consider_groupingsets_paths
  - create_agg_path
  - create_group_path
  - add_path
  - gather_grouping_paths
- Called from (representative examples):
  - create_ordinary_grouping_paths
  - standard_qp_extra

## Notes and Other Information
- The function supports both parallel and non-parallel grouping strategies through partial aggregation paths
- Path generation is controlled by GROUPING_CAN_USE_HASH and GROUPING_CAN_USE_SORT flags in the extra parameter
- When partitionwise aggregation is enabled, the function handles fully aggregated paths from child relations
- The function ensures optimal path selection by considering multiple grouping key orderings and aggregation strategies
- Location: src/backend/optimizer/plan/planner.c:7044-7278