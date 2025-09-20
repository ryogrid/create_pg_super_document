# create_partitionwise_grouping_paths

## Location
[src/backend/optimizer/plan/planner.c:7940-8083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L7940-L8083)

## Overview
Creates partitionwise grouping and aggregation paths for partitioned relations by breaking down aggregation into per-partition operations followed by combining results via append operations.

## Definition

```c
structure as is and then override the
		 * members specific to this child.
		 */
		memcpy(&child_extra, extra, sizeof(child_extra));
```
## Detailed Description
This function optimizes aggregation and grouping operations over partitioned relations by implementing partitionwise processing. It handles two main scenarios:

1. **Full partitionwise aggregation**: When all partition keys are included in the GROUP BY clause, each group's rows come from a single partition, allowing complete aggregation per partition followed by simple appending of results.

2. **Partial partitionwise aggregation**: When GROUP BY doesn't contain all partition keys, rows from a group may span multiple partitions. The function performs partial aggregation on each partition, appends the results, and then finalizes the aggregation.

The function iterates through each live partition of the input relation, creates child-specific grouping relations, and generates appropriate grouping paths. It translates expressions and qualifiers for each child partition using append relation information, then creates ordinary grouping paths for each child.

## Parameters / Member Variables
- : PlannerInfo containing global planning context and state information
- : RelOptInfo for the partitioned input relation to be grouped/aggregated
- : RelOptInfo for the final grouped relation that will contain fully aggregated results
- : RelOptInfo for partially grouped results (used in partial partitionwise aggregation)
- : AggClauseCosts structure containing cost estimates for aggregate functions
- : grouping_sets_data containing information about grouping sets operations
- : PartitionwiseAggregateType indicating whether to use full or partial partitionwise aggregation
- : GroupPathExtraData containing additional information like target lists and having qualifiers

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md)
  - IS_DUMMY_REL
  - [copy_pathtarget](copy_pathtarget.md)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md)
  - [make_grouping_rel](../m/make_grouping_rel.md)
  - [create_ordinary_grouping_paths](create_ordinary_grouping_paths.md)
  - [set_cheapest](../s/set_cheapest.md)
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md)
- Called from (representative examples):
  - [create_ordinary_grouping_paths](create_ordinary_grouping_paths.md)

## Notes and Other Information
- The function only processes live (non-dummy) partitions to avoid unnecessary work
- Expression translation is performed for each child partition to account for different column references
- Partial grouping validity is tracked - if any child cannot produce a partially grouped path, partial partitionwise aggregation is disabled
- The function is designed to be no worse than normal aggregation approaches and often performs better, especially when partition elimination can occur or when partial aggregation significantly reduces group counts