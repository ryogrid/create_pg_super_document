# ExecParallelHashIncreaseNumBatches

## Location
src/backend/executor/nodeHash.c: 1080 - 1311

## Overview
ExecParallelHashIncreaseNumBatches coordinates the dynamic expansion of batch count across all parallel participants in a hash join operation when memory pressure requires repartitioning of data into more batches.

## Definition


## Detailed Description
This function orchestrates a complex multi-phase operation that increases the number of batches in a parallel hash join when the current batching scheme becomes insufficient due to memory constraints. The function implements a state machine with multiple barrier synchronization phases to ensure all parallel workers coordinate properly during the repartitioning process.

The operation involves several coordinated phases:
1. **Election Phase (PHJ_GROW_BATCHES_ELECT)**: One participant is elected to prepare the new batch structure
2. **Reallocation Phase (PHJ_GROW_BATCHES_REALLOCATE)**: Wait for structural changes to complete
3. **Repartitioning Phase (PHJ_GROW_BATCHES_REPARTITION)**: All participants repartition their data
4. **Decision Phase (PHJ_GROW_BATCHES_DECIDE)**: Evaluate whether further growth is needed or beneficial
5. **Finish Phase (PHJ_GROW_BATCHES_FINISH)**: Complete the operation

The function handles the transition from single-batch to multi-batch mode specially, adjusting memory budgets and calculating optimal batch counts. It also implements safeguards against extreme data skew and prevents unbounded growth.

## Parameters / Member Variables
- : The HashJoinTable structure containing the parallel hash join state and batch information

## Dependencies
- Functions called/Symbols referenced:
  - BarrierPhase, BarrierArriveAndWait (barrier synchronization)
  - [ExecParallelHashCloseBatchAccessors](ExecParallelHashCloseBatchAccessors.md), ExecParallelHashEnsureBatchAccessors (batch management)
  - [ExecParallelHashJoinSetUpBatches](ExecParallelHashJoinSetUpBatches.md) (batch setup)
  - [ExecParallelHashRepartitionFirst](ExecParallelHashRepartitionFirst.md), ExecParallelHashRepartitionRest (repartitioning)
  - [ExecParallelHashMergeCounters](ExecParallelHashMergeCounters.md) (counter management)
  - [ExecParallelHashTableSetCurrentBatch](ExecParallelHashTableSetCurrentBatch.md) (batch selection)
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md) (memory management)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md), pg_prevpower2_32 (power-of-2 calculations)
  - dsa_allocate, dsa_free, dsa_get_address (shared memory management)
  - NthParallelHashJoinBatch (batch access)

- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecParallelHashTupleAlloc](ExecParallelHashTupleAlloc.md)
  - [ExecParallelHashTuplePrealloc](ExecParallelHashTuplePrealloc.md)

## Notes and Other Information
- This function is critical for handling memory pressure in parallel hash joins by dynamically increasing the number of batches
- The multi-phase barrier synchronization ensures all parallel workers remain coordinated during the complex repartitioning operation
- Special handling exists for the transition from single-batch (unlimited memory) to multi-batch mode
- The function includes logic to detect and handle extreme data skew that would make further repartitioning ineffective
- Growth can be disabled if repartitioning is not helping or if the maximum number of batches would be exceeded
- The function operates on shared memory structures accessible by all parallel participants