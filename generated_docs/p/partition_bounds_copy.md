# partition_bounds_copy

## Location
[src/backend/partitioning/partbounds.c:1002-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1002-L1117)

## Overview
Creates a deep copy of a PartitionBoundInfo structure, duplicating all data elements while respecting memory management constraints for long-lived contexts.

## Definition


## Detailed Description
This function creates a complete deep copy of a PartitionBoundInfo structure, carefully copying all data elements including datums, indexes, and metadata. It handles different partitioning strategies (hash, range, list) appropriately, using the partition key specification to determine data types and copying behavior. The function is designed to avoid catalog access and unwanted memory leaks in long-lived contexts.

Key copying behaviors:
- Allocates new memory for all structure elements
- Deep copies datum values using appropriate type-specific methods
- Handles range partition kinds (MINVALUE/MAXVALUE) specially
- For hash partitions, treats datums as int32 modulus/remainder pairs
- Copies interleaved partition bitmaps for list partitions

## Parameters / Member Variables
- : Source PartitionBoundInfo structure to copy from
- : PartitionKey containing partitioning metadata (data types, strategy, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [datumCopy](../d/datumCopy.md)
  - [bms_copy](../b/bms_copy.md)
  - [palloc](palloc.md)
  - memcpy
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - PartitionBoundInfoData
  - [PartitionKey](../P/PartitionKey.md)
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md)
- Constants used:
  - PARTITION_STRATEGY_HASH
  - PARTITION_STRATEGY_RANGE
  - PARTITION_STRATEGY_LIST
  - PARTITION_RANGE_DATUM_VALUE
- Called from:
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md)
  - partition_bound_has_default

## Notes and Other Information
- Designed for long-lived memory contexts - avoids catalog access and memory leaks
- Optimizes memory allocation by using single large arrays instead of many small ones
- [List](../L/List.md) partitions are constrained to single partition key (partnatts == 1)
- Hash partitions always use int32 for modulus/remainder values
- Only copies actual datum values for PARTITION_RANGE_DATUM_VALUE kinds
- Critical for relation descriptor building and caching infrastructure