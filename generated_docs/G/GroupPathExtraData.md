# GroupPathExtraData

## Location
src/include/nodes/pathnodes.h: 3303 - 3320

## Overview
GroupPathExtraData is a structure that contains extra information passed to subroutines of create_grouping_paths for controlling grouping and aggregation path generation in PostgreSQL's query optimizer.

## Definition


## Detailed Description
GroupPathExtraData serves as a container for various pieces of information needed during the creation of grouping and aggregation paths in PostgreSQL's query optimizer. This structure facilitates communication between different phases of query planning by bundling together configuration flags, cost information, query constraints, and partitioning strategies.

The structure is divided into two categories of data: constant data that remains unchanged once set (like cost estimates and capability flags), and variable data that may differ across partitions in a partitioned query execution plan. This design supports both traditional and partitionwise aggregation strategies.

The structure is particularly important for foreign data wrappers and partitioned table processing, where different execution strategies may be employed based on the capabilities of remote servers or the distribution of data across partitions.

## Parameters / Member Variables
- : Bit flags indicating what kinds of grouping operations are possible
- : True if the partial and final aggregation costs have been initialized
- : Cost estimates for partial aggregation operations
- : Cost estimates for finalization of aggregation operations
- : True if the target list can be safely executed in parallel
- : List of HAVING clause qualifiers to be applied after aggregation
- : List of columns/expressions to be projected in the result
- : Type of partitionwise aggregation being performed (see PartitionwiseAggregateType)

## Dependencies
- Functions called/Symbols referenced:
  - AggClauseCosts (for cost estimation structures)
  - [PartitionwiseAggregateType](../P/PartitionwiseAggregateType.md) (for partitioning strategy)
  - [Node](../N/Node.md) (for HAVING qualifiers)
  - [List](../L/List.md) (for target list management)

- Called from (representative examples):
  - standard_qp_extra (in planner.c for setting up extra data)
  - [create_grouping_paths](../c/create_grouping_paths.md) (in planner.c for path generation)
  - [create_ordinary_grouping_paths](../c/create_ordinary_grouping_paths.md) (for non-partitioned grouping)
  - [create_partitionwise_grouping_paths](../c/create_partitionwise_grouping_paths.md) (for partitioned table grouping)
  - postgres_fdw foreign data wrapper (for remote aggregation)

## Notes and Other Information
- Essential for coordinating complex aggregation strategies across the query optimizer
- Used extensively in partitioned table scenarios where different partitions may require different execution strategies
- The separation of constant vs. variable data supports efficient processing of partitioned queries
- Foreign data wrappers use this structure to determine appropriate server-side aggregation strategies
- Cost information helps the optimizer choose between different aggregation methods (hash vs. sort-based)
- The structure supports both parallel and sequential execution paths through the target_parallel_safe flag