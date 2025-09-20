# create_partial_grouping_paths

## Location
[src/backend/optimizer/plan/planner.c:7279-7577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L7279-L7577)

## Overview
Creates a new upper relation for partial aggregation results and populates it with appropriate paths that perform initial phases of aggregation, preparing data for subsequent finalization steps in parallel query processing.

## Definition

```c
static RelOptInfo *
create_partial_grouping_paths(PlannerInfo *root,
							  RelOptInfo *grouped_rel,
							  RelOptInfo *input_rel,
							  grouping_sets_data *gd,
							  GroupPathExtraData *extra,
							  bool force_rel_creation)
```
## Detailed Description
This function is a key component of PostgreSQL's parallel aggregation strategy. It creates an intermediate relation (UPPERREL_PARTIAL_GROUP_AGG) that represents partially aggregated results requiring subsequent finalization. The function handles:

1. **Partial aggregation path creation**: Generates paths using AGGSPLIT_INITIAL_SERIAL for both sorted and hashed approaches
2. **Parallel processing support**: Creates both partial and non-partial paths depending on parallelism capabilities
3. **Target list optimization**: Builds specialized target lists including necessary Vars and Aggrefs for HAVING clauses
4. **Cost estimation**: Calculates separate costs for partial and final aggregation phases
5. **FDW integration**: Allows foreign data wrappers to contribute custom partial grouping paths

The function intelligently determines whether partial aggregation is beneficial based on available input paths, parallelism settings, and partitionwise aggregation configuration.

## Parameters / Member Variables
- : PlannerInfo containing query planning context and metadata
- : RelOptInfo representing the final grouped relation that will receive finalized results
- : RelOptInfo representing the input relation to be partially aggregated
- : grouping_sets_data containing grouping sets configuration information
- : GroupPathExtraData containing flags, costs, and additional planning parameters
- : Boolean flag to force creation of the relation even when optimization suggests it's unnecessary

## Dependencies
- Functions called/Symbols referenced:
  - fetch_upper_rel
  - [make_partial_grouping_target](../m/make_partial_grouping_target.md)
  - [get_agg_clause_costs](../g/get_agg_clause_costs.md)
  - [get_number_of_groups](../g/get_number_of_groups.md)
  - [get_useful_group_keys_orderings](../g/get_useful_group_keys_orderings.md)
  - [make_ordered_path](../m/make_ordered_path.md)
  - [create_agg_path](create_agg_path.md)
  - [create_group_path](create_group_path.md)
  - [add_path](../a/add_path.md)
  - [add_partial_path](../a/add_partial_path.md)
- Called from (representative examples):
  - [create_ordinary_grouping_paths](create_ordinary_grouping_paths.md)
  - standard_qp_extra

## Notes and Other Information
- Returns NULL if no real benefit is found in creating partial aggregation paths (unless force_rel_creation is true)
- Sets up both partial_costs and final_costs for accurate cost estimation of two-phase aggregation
- Supports both partitionwise aggregation (PARTITIONWISE_AGGREGATE_PARTIAL) and regular parallel processing
- The created relation requires subsequent gather_grouping_paths and set_cheapest calls to finalize path lists
- All generated paths use AGGSPLIT_INITIAL_SERIAL aggregation mode, expecting later AGGSPLIT_FINAL_DESERIAL finalization
- Location: src/backend/optimizer/plan/planner.c:7279-7577