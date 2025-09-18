# partition_bounds_equal

## Location
[src/backend/partitioning/partbounds.c:896-1001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L896-L1001)

## Overview
Determines if two partition bound collections are logically equal by comparing their structural elements and data content.

## Definition


## Detailed Description
This function performs a deep comparison of two PartitionBoundInfo structures to determine logical equality. It is used in the relcache keep logic and for comparing partition bounds between different relations. The function compares structural elements first (strategy, counts, indexes) then performs detailed datum-by-datum comparison based on the partitioning strategy.

For hash partitions, it leverages the fact that if indexes arrays match, the bounds are equivalent due to the modulus/remainder organization. For range and list partitions, it performs element-wise comparison of both bound kinds and actual datum values using safe comparison methods.

## Parameters / Member Variables
- : Number of partition attributes 
- : Array of type lengths for each partition attribute
- : Array indicating if each partition attribute type is passed by value
- : First PartitionBoundInfo structure to compare
- : Second PartitionBoundInfo structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [datumIsEqual](../d/datumIsEqual.md)
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - PARTITION_STRATEGY_HASH
  - PARTITION_RANGE_DATUM_VALUE
- Called from:
  - [compute_partition_bounds](../c/compute_partition_bounds.md)
  - partition_bound_has_default

## Notes and Other Information
- Uses datumIsEqual() instead of partitioning operators for safety in aborted transaction contexts
- For hash partitions, relies on indexes array comparison due to modulus/remainder organization
- Handles non-finite bounds (MINVALUE/MAXVALUE) specially for range partitions
- Designed to detect ANY change to partition bounds, not just semantically significant ones
- Critical for relcache invalidation logic to ensure cache consistency