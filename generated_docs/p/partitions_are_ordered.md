# partitions_are_ordered

## Location
src/backend/partitioning/partbounds.c: 2852 - 2895

## Overview
Determines whether partitions are ordered such that earlier partitions contain keys strictly less than later ones, enabling sequential scan optimizations.

## Definition
```c
bool partitions_are_ordered(PartitionBoundInfo boundinfo, Bitmapset *live_parts)
```

## Detailed Description
This function analyzes partition bounds to determine if the partitions can be scanned in definition order to provide sequentially ordered data. For RANGE partitions, ordering is guaranteed by design unless a DEFAULT partition is included in the live set. For LIST partitions, ordering exists when no interleaved partitions overlap with the live partition set. HASH partitions never provide ordering guarantees. This information is crucial for query optimization, particularly for enabling Append nodes instead of more expensive MergeAppend nodes when data ordering is preserved.

## Parameters / Member Variables
- `boundinfo`: Partition bounds information containing strategy and boundary details
- `live_parts`: Bitmapset indicating which partitions are relevant for the current query

## Dependencies
- Functions called/Symbols referenced:
  - PartitionBoundInfo
  - PARTITION_STRATEGY_RANGE
  - partition_bound_has_default
  - bms_is_member
  - PARTITION_STRATEGY_LIST
  - bms_overlap
  - PARTITION_STRATEGY_HASH
- Called from (representative examples):
  - generate_orderedappend_paths
  - build_partition_pathkeys

## Notes and Other Information
- Returns true only when partitions guarantee sequential ordering of data
- RANGE partitions provide natural ordering except when DEFAULT partitions are involved
- LIST partitions can provide ordering when interleaved partitions are excluded
- HASH partitions never provide ordering due to their distribution strategy
- Critical for PostgreSQL's query optimization to choose efficient execution plans
- Located in src/backend/partitioning/partbounds.c:2852-2895