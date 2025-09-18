# build_partition_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 917 - 997

## Overview
Builds a pathkeys list that describes the ordering induced by the partitions of a partitioned relation, supporting both forward and backward scan directions.

## Definition


## Detailed Description
This function constructs a list of PathKey objects that represent the sort order implied by the partitioning scheme of a relation. It iterates through each partition key column and attempts to create canonical pathkeys that capture the ordering relationship between partitions.

The function assumes that partitions are properly ordered (verified by partitions_are_ordered()) and handles NULL partition placement by treating scans like NULLS LAST indexes. For each partition key column, it tries to create a pathkey using the partition's operator family, collation, and data type information.

The function stops building pathkeys when it encounters a partition key that cannot be represented as a useful sort order for the current query, unless the key is a boolean constant that can be optimized away.

## Parameters / Member Variables
- : PlannerInfo containing query planning context and equivalence classes
- : RelOptInfo representing the partitioned relation (must be a simple base relation)
- : ScanDirection indicating forward or backward scan direction
- : Output parameter set to true if pathkeys only cover a prefix of partition keys, false if all partition key columns are included

## Dependencies
- Functions called/Symbols referenced:
  - partitions_are_ordered (to verify partition ordering)
  - IS_SIMPLE_REL (to validate relation type)
  - make_pathkey_from_sortinfo (to create canonical pathkeys)
  - ScanDirectionIsBackward (to handle scan direction)
  - pathkey_is_redundant (to avoid duplicate pathkeys)
  - partkey_is_bool_constant_for_query (to handle boolean partition keys)
- Called from (representative examples):
  - generate_orderedappend_paths

## Notes and Other Information
- Currently only supports simple base relations (not joins or subqueries)
- Assumes NULL partitions are listed last in the PartitionDesc
- Boolean partition keys receive special treatment and may be skipped if they represent constant conditions
- Part of PostgreSQL's partition-wise join and append optimization system
- The returned pathkeys can be used to determine if an ordered append operation is beneficial