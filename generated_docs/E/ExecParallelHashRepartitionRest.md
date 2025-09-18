# ExecParallelHashRepartitionRest

## Location
[src/backend/executor/nodeHash.c:1379-1438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1379-L1438)

## Overview
ExecParallelHashRepartitionRest assists in repartitioning inner batches 1 through n from the previous generation of batches during a parallel hash join batch increase operation.

## Definition


## Detailed Description
This function handles the second phase of tuple repartitioning when the number of batches is increased in a parallel hash join. While ExecParallelHashRepartitionFirst handles tuples in memory (batch 0), this function processes tuples stored on disk in batches 1 through n from the previous generation.

The function operates by:
1. Accessing the previous generation of batches using shared memory addresses
2. Attaching to the shared tuplestore accessors for each old batch (1..n)
3. Performing parallel scans of each old batch's tuples
4. Recalculating bucket and batch assignments for each tuple based on new batch count
5. Writing tuples to their new target batches using the shared tuplestore
6. Updating batch statistics (estimated size, tuple counts)

This cooperative repartitioning allows multiple parallel workers to efficiently redistribute large amounts of data stored on disk across the new batch structure, ensuring balanced workload distribution and optimal memory usage.

## Parameters / Member Variables
- : The HashJoinTable containing parallel hash join state, batch information, and access to shared memory structures

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md) (shared memory address resolution)
  - NthParallelHashJoinBatch (batch accessor function)
  - [sts_attach](../s/sts_attach.md), sts_begin_parallel_scan, sts_parallel_scan_next, sts_end_parallel_scan (shared tuplestore operations)
  - ParallelHashJoinBatchInner (batch inner structure accessor)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md) (hash-to-bucket/batch mapping)
  - [sts_puttuple](../s/sts_puttuple.md) (tuple storage to disk)
  - palloc0_array, pfree (memory management)
  - CHECK_FOR_INTERRUPTS (interruption handling)
  - HJTUPLE_OVERHEAD, MAXALIGN (tuple size calculations)

- Called from (representative examples):
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md)

## Notes and Other Information
- This function complements ExecParallelHashRepartitionFirst by handling disk-based batches while the former handles memory-resident batch 0
- Each parallel worker attaches to the shared tuplestores using their ParallelWorkerNumber + 1 as the participant ID
- The function performs parallel scanning, allowing multiple workers to cooperatively process large batches stored on disk
- Tuple counts are maintained for both old batches (old_ntuples) and new target batches (ntuples) for proper statistics tracking
- The function includes interrupt checking to allow for query cancellation during long repartitioning operations
- Memory allocated for tuplestore accessors is properly freed after processing to prevent memory leaks
- Only processes batches 1..n, as batch 0 is handled separately by ExecParallelHashRepartitionFirst