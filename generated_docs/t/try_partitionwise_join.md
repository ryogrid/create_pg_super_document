# try_partitionwise_join

## Location
src/backend/optimizer/path/joinrels.c: 1479 - 1693

## Overview
Attempts to perform partitionwise join optimization by breaking down a join between two partitioned relations into joins between matching partitions.

## Definition
static void try_partitionwise_join(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2, RelOptInfo *joinrel, SpecialJoinInfo *parent_sjinfo, List *parent_restrictlist)

## Detailed Description
This function implements the core logic for partitionwise join optimization, a technique that can significantly improve join performance when joining partitioned tables. The function determines whether a join between two partitioned relations can be decomposed into separate joins between corresponding partitions.

Partitionwise join is possible when:
1. Both joining relations have the same partitioning scheme
2. There exists an equi-join between the partition keys of the two relations

The function works in two main phases:
1. Creates RelOptInfos for joins between matching partitions (child-joins) and adds paths to them
2. Later, Append or MergeAppend paths are constructed across the set of child joins (by generate_partitionwise_join_paths)

For each partition pair, the function:
- Checks if the partition segments can be safely ignored (e.g., empty partitions)
- Constructs child SpecialJoinInfo and restrictlist structures by translating parent structures
- Creates or finds child join RelOptInfo structures
- Populates the child joins with appropriate paths

## Parameters / Member Variables
- : PlannerInfo containing global planner state and context
- : First partitioned relation to join
- : Second partitioned relation to join  
- : The target join relation that will contain the partitionwise join
- : SpecialJoinInfo for the parent join operation
- : List of join restriction clauses from the parent join

## Dependencies
- Functions called/Symbols referenced:
  - compute_partition_bounds
  - build_child_join_sjinfo
  - free_child_join_sjinfo
  - build_child_join_rel
  - populate_joinrel_with_paths
  - find_appinfos_by_relids
  - adjust_appendrel_attrs
  - adjust_child_relids
  - IS_PARTITIONED_REL
  - IS_SIMPLE_REL
  - IS_DUMMY_REL
- Called from (representative examples):
  - populate_joinrel_with_paths

## Notes and Other Information
- Guards against stack overflow due to overly deep partition hierarchies
- Handles various join types (INNER, LEFT, FULL, SEMI, ANTI) with appropriate empty partition logic
- Fails gracefully when partitionwise join is not feasible by setting joinrel->nparts = 0
- Maintains partition bounds information and live partition tracking
- Uses AppendRelInfo structures to translate expressions between parent and child relations
- Part of PostgreSQL's advanced join optimization framework for partitioned tables