# build_joinrel_partition_info

## Location
[src/backend/optimizer/util/relnode.c:2017-2089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L2017-L2089)

## Overview
Determines if two relations being joined can use partitionwise join and initializes the partitioning information for the resulting join relation if partitionwise join is applicable.

## Definition

```c
static void
build_joinrel_partition_info(PlannerInfo *root,
							 RelOptInfo *joinrel, RelOptInfo *outer_rel,
							 RelOptInfo *inner_rel, SpecialJoinInfo *sjinfo,
							 List *restrictlist)
```
## Detailed Description
This function is responsible for setting up partition information for a join relation when partitionwise join optimization is possible. It validates that both input relations are partitioned with matching partition schemes and that there exists an equi-join condition on the partition keys. The function performs comprehensive checks to ensure partitionwise join is viable:

1. Verifies that partitionwise join is enabled ()
2. Checks that both input relations are partitioned and have 
3. Ensures partition schemes match between the relations
4. Confirms existence of equi-join conditions on partition keys via 

If all conditions are met, the function sets up the join relation's partitioning metadata by copying the partition scheme from the input relations and calling  to establish partition key expressions for the join.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and configuration
- : The join relation being constructed that will potentially inherit partition information  
- : The outer relation in the join operation
- : The inner relation in the join operation
- : SpecialJoinInfo containing join type and other join-specific metadata
- : List of join restriction clauses used to determine equi-join conditions

## Dependencies
- Functions called/Symbols referenced:
  - : Determines if equi-join exists on partition keys
  - : Sets up partition key expressions for the join relation
  - : Macro to check if a relation is partitioned
  - : Structure containing join metadata
  - : Partitioning scheme definition
- Called from (representative examples):
  - : Main join relation construction function
  - : Child join relation construction for partitioned tables

## Notes and Other Information
- This function is only called once per joinrel to avoid duplicate initialization
- The actual partition bounds, partition count, and child relations are computed later in 
- The function includes assertions to ensure the join relation doesn't already have partitioning information set
- Partitionwise join is a critical PostgreSQL optimization that allows joins to be executed in parallel across matching partitions, significantly improving performance for large partitioned tables