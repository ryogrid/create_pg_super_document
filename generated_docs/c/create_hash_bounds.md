# create_hash_bounds

## Location
[src/backend/partitioning/partbounds.c:347-435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L347-L435)

## Overview
Creates a PartitionBoundInfo structure specifically for hash partitioned tables, converting hash partition bound specifications into the internal representation.

## Definition
```c
static PartitionBoundInfo create_hash_bounds(PartitionBoundSpec **boundspecs, int nparts, PartitionKey key, int **mapping)
```

## Detailed Description
This function implements hash partition bounds creation by processing an array of hash partition specifications and building the internal PartitionBoundInfo structure. For hash partitioning, each partition is defined by a modulus and remainder pair. The function:

1. Converts PartitionBoundSpec nodes to internal PartitionHashBound representations
2. Sorts the bounds in ascending order by modulus using qsort_partition_hbound_cmp
3. Creates a mapping of hash values to partition indexes based on the greatest modulus
4. Builds the datums array containing modulus/remainder pairs for each partition
5. Sets up the indexes array to map hash values to their corresponding partitions

The resulting structure enables efficient hash value to partition mapping during query execution and partition pruning.

## Parameters / Member Variables
- `boundspecs`: Array of PartitionBoundSpec pointers containing hash partition specifications (modulus/remainder pairs)
- `nparts`: Number of hash partitions to process
- `key`: PartitionKey containing the partitioning strategy and key information
- `mapping`: Output parameter - array mapping original partition indexes to canonical sorted indexes

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - qsort
  - [qsort_partition_hbound_cmp](../q/qsort_partition_hbound_cmp.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - PARTITION_STRATEGY_HASH
  - [PartitionHashBound](../P/PartitionHashBound.md)
  - PartitionBoundInfoData
- Called from (representative examples):
  - [partition_bounds_create](../p/partition_bounds_create.md) (src/backend/partitioning/partbounds.c:329)

## Notes and Other Information
- Static function, only accessible within partbounds.c
- Hash partitions do not support NULL or DEFAULT partitions (null_index and default_index set to -1)
- The indexes array size is determined by the greatest modulus among all partitions
- Uses a single large Datum array allocation and assigns portions to each partition for efficiency
- Validates that all bound specifications use PARTITION_STRATEGY_HASH strategy
- The remainder values are distributed across the indexes array using modular arithmetic
- Ensures no hash value overlaps between partitions through assertions