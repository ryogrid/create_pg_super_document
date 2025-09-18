# partition_bounds_create

## Location
src/backend/partitioning/partbounds.c: 299 - 346

## Overview
Builds a PartitionBoundInfo structure from an array of PartitionBoundSpec nodes, creating the internal representation of partition bounds with canonical ordering and mapping.

## Definition
```c
PartitionBoundInfo partition_bounds_create(PartitionBoundSpec **boundspecs, int nparts, PartitionKey key, int **mapping)
```

## Detailed Description
This function serves as the main entry point for converting parser-level partition bound specifications into the internal PartitionBoundInfo representation used throughout PostgreSQL's partition management system. It processes an array of partition bound specifications and creates a consolidated structure containing:

1. A sorted 'datums' array with Datum representations of individual bounds in canonical order
2. An 'indexes' array indicating the canonical positions of partitions
3. A mapping array that translates original partition indexes to canonical indexes

The function acts as a dispatcher, delegating to strategy-specific functions (create_hash_bounds, create_list_bounds, create_range_bounds) based on the partitioning method. The canonical ordering is defined by the qsort_partition_* functions specific to each partitioning strategy.

## Parameters / Member Variables
- `boundspecs`: Array of PartitionBoundSpec pointers containing the partition bound specifications to process
- `nparts`: Number of partitions (length of boundspecs array)
- `key`: PartitionKey containing the partitioning strategy and key information
- `mapping`: Output parameter - pointer to an array that maps original partition indexes to canonical indexes

## Dependencies
- Functions called/Symbols referenced:
  - [create_hash_bounds](../c/create_hash_bounds.md)
  - [create_list_bounds](../c/create_list_bounds.md)
  - [create_range_bounds](../c/create_range_bounds.md)
  - PARTITION_STRATEGY_HASH
  - PARTITION_STRATEGY_LIST
  - PARTITION_STRATEGY_RANGE
  - [palloc](palloc.md)
- Called from (representative examples):
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md) (src/backend/partitioning/partdesc.c:311)

## Notes and Other Information
- All returned objects are allocated in the current memory context
- The mapping array is initialized with -1 values and filled by the strategy-specific functions
- Requires nparts > 0 (enforced by assertion)
- The function performs deduplication for range bounds as part of preprocessing
- The canonical ordering enables efficient partition pruning and lookup operations
- Critical component in the partition descriptor building process