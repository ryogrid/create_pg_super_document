# ExecHashGetBucketAndBatch

## Location
[src/backend/executor/nodeHash.c:1939-1970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1939-L1970)

## Overview
Determines the bucket number and batch number for a given hash value in a hash join table, using bit manipulation techniques to ensure proper distribution across buckets and batches.

## Definition
```c
void ExecHashGetBucketAndBatch(HashJoinTable hashtable,
                               uint32 hashvalue,
                               int *bucketno,
                               int *batchno)
```

## Detailed Description
This function implements the core hash distribution logic for PostgreSQL hash joins. It maps a hash value to both a bucket number (for hash chain placement) and batch number (for memory management during large joins). The algorithm ensures that:

- Bucket numbers remain stable even when the number of batches increases during execution
- Batch numbers can only stay the same or increase when nbatch grows
- Both bucket and batch calculations use efficient bit operations assuming power-of-2 sizing

The algorithm uses:
- `bucketno = hashvalue MOD nbuckets` (using bit masking)
- `batchno = ROR(hashvalue, log2_nbuckets) MOD nbatch` (using rotation and bit masking)

When batching is not active (nbatch == 1), all tuples are assigned to batch 0. For larger joins requiring batching, the function uses bit rotation to steal bits from the bucket number for batch assignment, prioritizing the ability to create more batches over shorter bucket chains.

## Parameters / Member Variables
- `hashtable`: The hash join table containing bucket and batch configuration
- `hashvalue`: The 32-bit hash value to be mapped
- `bucketno`: Output parameter for the calculated bucket number
- `batchno`: Output parameter for the calculated batch number

## Dependencies
- Functions called/Symbols referenced:
  - [HashJoinTable](../H/HashJoinTable.md) (struct type)
  - [pg_rotate_right32](../p/pg_rotate_right32.md) (bit rotation function)
- Called from (representative examples):
  - [ExecHashIncreaseNumBatches](ExecHashIncreaseNumBatches.md)
  - [ExecHashTableInsert](ExecHashTableInsert.md)
  - [ExecParallelHashTableInsert](ExecParallelHashTableInsert.md)
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)
  - [ExecParallelHashJoinPartitionOuter](ExecParallelHashJoinPartitionOuter.md)

## Notes and Other Information
- Assumes hash functions produce well-randomized output bits to avoid skewed distribution
- nbuckets and log2_nbuckets may change dynamically when nbatch == 1, but become fixed once batching starts
- nbatch is always a power of 2 and only increases by doubling
- In very large joins, bit rotation may cause batch numbers to use bits from bucket numbers when virtual buckets exceed 2^32
- The function prioritizes maintaining batch creation capability over minimizing bucket chain length