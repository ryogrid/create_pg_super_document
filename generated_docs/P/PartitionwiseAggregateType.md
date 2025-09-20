# PartitionwiseAggregateType

## Location
[src/include/nodes/pathnodes.h:3275-3302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3275-L3302)

## Overview
PartitionwiseAggregateType is an enumeration that specifies the strategy for performing aggregation operations across partitioned tables in PostgreSQL's query optimizer.

## Definition

```c
typedef struct
{
	/* Data which remains constant once set. */
	int			flags;
	bool		partial_costs_set;
	AggClauseCosts agg_partial_costs;
	AggClauseCosts agg_final_costs;

	/* Data which may differ across partitions. */
	bool		target_parallel_safe;
	Node	   *havingQual;
	List	   *targetList;
	PartitionwiseAggregateType patype;
} GroupPathExtraData;
```
## Detailed Description
PartitionwiseAggregateType defines three distinct strategies for handling aggregation operations when working with partitioned tables in PostgreSQL. This enumeration is crucial for optimizing query performance by allowing the optimizer to choose the most efficient approach for aggregating data across multiple partitions.

The NONE option disables partitionwise aggregation entirely, falling back to traditional aggregation methods. The FULL option performs complete aggregation on each partition separately and then appends the results, which is efficient when each partition can produce final results independently. The PARTIAL option uses a two-phase approach: first performing partial aggregation on each partition, then combining and finalizing the results, which is beneficial for aggregates that can be computed incrementally.

The choice between these methods depends on factors such as the type of aggregate functions used, data distribution across partitions, available memory, and parallelization opportunities.

## Parameters / Member Variables
- : Partitionwise aggregation is not used; traditional aggregation methods are employed
- : Aggregate each partition separately and append the results (complete aggregation per partition)
- : Partially aggregate each partition separately, append results, then finalize aggregation (two-phase approach)

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum, so it doesn't directly reference other symbols)

- Called from (representative examples):
  - [create_ordinary_grouping_paths](../c/create_ordinary_grouping_paths.md) (in planner.c for grouping path creation)
  - [create_partitionwise_grouping_paths](../c/create_partitionwise_grouping_paths.md) (in planner.c for partitioned aggregation)
  - [GroupPathExtraData](../G/GroupPathExtraData.md) structure (as patype field)
  - Foreign data wrapper implementations (postgres_fdw)

## Notes and Other Information
- Used within GroupPathExtraData structure to control partitionwise aggregation behavior
- FULL aggregation requires that aggregate functions can be computed completely within each partition
- PARTIAL aggregation supports combine functions and is useful for parallel aggregation scenarios
- The optimizer automatically determines the appropriate type based on query characteristics and system capabilities
- Critical for performance optimization in partitioned table scenarios with large datasets
- Particularly beneficial when partitions are stored on different nodes or storage devices
- Foreign data wrappers like postgres_fdw must handle partitionwise aggregation appropriately