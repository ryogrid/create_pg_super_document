# create_ordinary_grouping_paths

## Location
src/backend/optimizer/plan/planner.c: 4071 - 4210

## Overview
Creates grouping paths for ordinary (non-degenerate) GROUP BY cases, handling both sorted and hashed aggregation strategies while supporting partial aggregation and partitionwise aggregation optimization techniques.

## Definition


## Detailed Description
This function is responsible for creating execution paths for ordinary GROUP BY operations in PostgreSQL's query planner. It considers both sorted and hashed aggregation strategies simultaneously to ensure at least one viable approach is found, and provides appropriate error messages when neither works.

The function supports several advanced optimization techniques:
- **Partial aggregation**: Creates partially grouped paths that can be further processed in parallel
- **Partitionwise aggregation**: Leverages table partitioning to perform aggregation on individual partitions
- **Parallel processing**: Integrates with PostgreSQL's parallel query execution framework

The function follows a structured approach: first generating any possible partially grouped paths, then applying partitionwise aggregation if applicable, gathering partial paths, estimating group counts, and finally building the complete set of grouping paths.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and query information
- : RelOptInfo representing the input relation to be grouped
- : RelOptInfo representing the target grouped relation to receive new paths
- : Cost estimates for aggregate functions used in the query
- : grouping_sets_data structure containing grouping set information
- : GroupPathExtraData containing additional parameters like target list and flags
- : Output parameter set to the created partially grouped relation or NULL

## Dependencies
- Functions called/Symbols referenced:
  - group_by_has_partkey
  - create_partial_grouping_paths
  - create_partitionwise_grouping_paths
  - gather_grouping_paths
  - get_number_of_groups
  - add_paths_to_grouping_rel
  - set_cheapest
- Called from (representative examples):
  - create_grouping_paths
  - create_partitionwise_grouping_paths

## Notes and Other Information
- The function must consider both sorted and hashed aggregation to provide comprehensive error reporting when aggregation fails
- Hashtable size considerations should not prevent using hashing if sorting is impossible
- Supports integration with Foreign Data Wrappers (FDW) through GetForeignUpperPaths callback
- Provides extension points through create_upper_paths_hook for custom upper-level path creation
- Throws a helpful error message when no GROUP BY implementation can be found, specifically noting datatype compatibility issues between hashing and sorting requirements