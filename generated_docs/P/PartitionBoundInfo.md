# PartitionBoundInfo

## Location
src/include/partitioning/partdefs.h: 16 - 17

## Overview
A pointer to PartitionBoundInfoData structure that encapsulates a set of partition bounds, typically associated with partitioned tables or used to represent virtual partitioned tables within the planner.

## Definition


## Detailed Description
PartitionBoundInfo is a pointer type to the PartitionBoundInfoData structure, which contains comprehensive information about partition boundaries for different partitioning strategies (hash, list, or range). The structure stores partition boundary data in a format optimized for efficient partition pruning and routing operations.

The underlying PartitionBoundInfoData structure maintains:
- Arrays of partition boundary datums organized for efficient binary search
- Index mappings to actual partition numbers
- Special handling for NULL and DEFAULT partitions
- Strategy-specific optimizations for hash, list, and range partitioning

For range partitioning, datums represent boundary points with minimal storage (upper bounds shared as lower bounds). For list partitioning, each datum maps directly to a partition index. For hash partitioning, datums store modulus/remainder pairs for hash-based routing.

## Parameters / Member Variables
(This is a typedef pointer, see PartitionBoundInfoData for actual structure members)

## Dependencies
- Functions called/Symbols referenced:
  - PartitionBoundInfoData (underlying structure)
  - PartitionStrategy (partitioning strategy enum)
  - Datum (PostgreSQL datum type)
  - PartitionRangeDatumKind (range boundary type indicators)
  - Bitmapset (for tracking interleaved partitions)

- Called from (representative examples):
  - RelationBuildPartitionDesc (partition descriptor construction)
  - get_partition_for_tuple (partition routing)
  - partition_bounds_create (boundary creation)
  - partition_bounds_merge (boundary merging for joins)
  - get_matching_hash_bounds/get_matching_list_bounds/get_matching_range_bounds (partition pruning)

## Notes and Other Information
- The structure is designed for efficient binary search operations during partition pruning
- Special handling exists for NULL values (null_index) and DEFAULT partitions (default_index)
- For LIST partitions, tracks potentially interleaved partitions to optimize pruning decisions
- Memory layout is optimized for cache-friendly access patterns during partition lookups
- Used extensively in both executor (for tuple routing) and planner (for join partitioning and pruning)