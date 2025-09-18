# partition_bounds_copy

## Location
src/backend/partitioning/partbounds.c: 1002 - 1117

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
  - datumCopy
  - bms_copy
  - palloc
  - memcpy
- Data types used:
  - PartitionBoundInfo
  - PartitionBoundInfoData
  - PartitionKey
  - PartitionRangeDatumKind
- Constants used:
  - PARTITION_STRATEGY_HASH
  - PARTITION_STRATEGY_RANGE
  - PARTITION_STRATEGY_LIST
  - PARTITION_RANGE_DATUM_VALUE
- Called from:
  - RelationBuildPartitionDesc
  - partition_bound_has_default

## Notes and Other Information
- Designed for long-lived memory contexts - avoids catalog access and memory leaks
- Optimizes memory allocation by using single large arrays instead of many small ones
- List partitions are constrained to single partition key (partnatts == 1)
- Hash partitions always use int32 for modulus/remainder values
- Only copies actual datum values for PARTITION_RANGE_DATUM_VALUE kinds
- Critical for relation descriptor building and caching infrastructure