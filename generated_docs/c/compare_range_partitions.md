# compare_range_partitions

## Location
src/backend/partitioning/partbounds.c: 2662 - 2710

## Overview
Compares the bounds of two range partitions and determines if they overlap, while also providing comparison results for both lower and upper bounds.

## Definition


## Detailed Description
This function performs overlap detection between two range partitions by comparing their boundary values. It implements a comprehensive comparison algorithm that not only determines whether the partitions overlap but also provides detailed comparison results for both lower and upper bounds. The function uses early termination optimization - if the outer partition's upper bound is lower than the inner partition's lower bound, or if the outer partition's lower bound is higher than the inner partition's upper bound, the partitions cannot overlap. For overlapping cases, it computes precise comparison values for both boundary pairs.

## Parameters / Member Variables
- : Number of partition key attributes
- : Array of comparison functions for partition key types  
- : Array of collation OIDs for partition key attributes
- : Lower bound of the outer (first) partition
- : Upper bound of the outer (first) partition
- : Lower bound of the inner (second) partition
- : Upper bound of the inner (second) partition
- : Output parameter for lower bound comparison result (-1, 0, or 1)
- : Output parameter for upper bound comparison result (-1, 0, or 1)

## Dependencies
- Functions called/Symbols referenced:
  - compare_range_bounds
  - [PartitionRangeBound](../P/PartitionRangeBound.md)
- Called from (representative examples):
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- This is a static function within partbounds.c, used internally for partition bound operations
- The function implements efficient non-overlap detection using early termination
- Comparison values follow standard convention: -1 (less than), 0 (equal), 1 (greater than)
- Essential for PostgreSQL's range partitioning logic and partition pruning optimizations
- Located in src/backend/partitioning/partbounds.c:2662-2710