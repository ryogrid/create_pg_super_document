# get_partition_for_tuple

## Location
[src/backend/executor/execPartition.c:1391-1610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L1391-L1610)

## Overview
Finds the partition of a relation that accepts the specified partition key values, implementing an optimized lookup algorithm with caching for improved performance on repeated searches.

## Definition
```c
static int get_partition_for_tuple(PartitionDispatch pd, Datum *values, bool *isnull)
```

## Detailed Description
This function determines which partition should receive a tuple based on its partition key values. It implements different algorithms based on the partitioning strategy:

**Hash Partitioning**: Computes a hash value and uses modulo arithmetic to find the target partition directly without caching.

**List Partitioning**: Uses binary search to locate the matching partition value, with special handling for NULL values. Implements caching optimization for frequently accessed partitions.

**Range Partitioning**: Uses binary search to find the appropriate range boundary, with caching for performance. Handles NULL values by directing them to the DEFAULT partition.

**Caching Mechanism**: For LIST and RANGE partitioned tables, the function implements an intelligent caching system. When the same partition is found PARTITION_CACHED_FIND_THRESHOLD times consecutively, subsequent lookups first check if the values still belong to the cached partition before performing binary search. This optimization significantly improves performance for workloads with temporal locality.

## Parameters / Member Variables
- `pd`: PartitionDispatch object containing partition key information, partition description, and caching state
- `values`: Array of Datum values representing the partition key to look up
- `isnull`: Array of boolean flags indicating which partition key values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - [compute_partition_hash_value](../c/compute_partition_hash_value.md)
  - partition_bound_accepts_nulls
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [partition_list_bsearch](../p/partition_list_bsearch.md)
  - [partition_rbound_datum_cmp](../p/partition_rbound_datum_cmp.md)
  - [partition_range_datum_bsearch](../p/partition_range_datum_bsearch.md)
- Called from (representative examples):
  - [ExecFindPartition](../E/ExecFindPartition.md) (the main entry point for partition lookup during tuple routing)

## Notes and Other Information
- Returns partition index (>= 0 and < partdesc->nparts) if a matching partition is found, or -1 if no match
- Caching is not implemented for HASH partitioning as the computation is already very fast
- The DEFAULT partition is returned when no specific partition matches the key values
- For NULL partition keys in RANGE partitioning, the function automatically falls through to the DEFAULT partition
- Cache hit validation for LIST partitioning uses the partition's comparison function to verify datum equality
- Cache hit validation for RANGE partitioning checks both lower and upper bounds to ensure the value still falls within the cached partition's range
- The caching mechanism maintains statistics on consecutive hits to the same partition (last_found_count, last_found_part_index, last_found_datum_index)
- This function can be expensive for tables with many LIST or RANGE partitions due to binary search overhead, hence the caching optimization