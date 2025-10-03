# ExecParallelHashMergeCounters

## Location
[src/backend/executor/nodeHash.c:1439-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1439-L1468)

## Overview
ExecParallelHashMergeCounters transfers backend-local per-batch counters to the shared totals, synchronizing statistics across all parallel workers in a hash join operation.

## Definition

```c
static void
ExecParallelHashMergeCounters(HashJoinTable hashtable)
```
## Detailed Description
This function consolidates batch statistics that have been maintained locally by each parallel worker into the shared parallel state. Each worker maintains local counters for efficiency during normal operation to avoid lock contention, but at certain synchronization points (such as during batch repartitioning), these local counters need to be merged into the shared state so all workers have a consistent view of the hash table statistics.

The function performs the following operations under exclusive lock protection:
1. Acquires an exclusive lightweight lock on the parallel state
2. Resets the global total_tuples counter 
3. Iterates through all batches, adding local counters to shared counters
4. Resets local counters to zero after merging
5. Recalculates the total tuple count across all batches
6. Releases the lock

This synchronization is essential for making informed decisions about memory usage, batch sizing, and whether further repartitioning is necessary.

## Parameters / Member Variables
- : The HashJoinTable containing both local batch accessors and shared parallel state information

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md), LWLockRelease (lightweight locking)
  - [ParallelHashJoinState](../P/ParallelHashJoinState.md), ParallelHashJoinBatchAccessor (parallel state structures)

- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md)

## Notes and Other Information
- This function is called at synchronization points where all workers need consistent batch statistics
- The exclusive lock ensures atomic updates to shared counters, preventing race conditions
- Local counters are reset to zero after merging to prepare for the next phase of operation
- The function merges four types of counters: size, estimated_size, ntuples, and old_ntuples
- The total_tuples field provides a quick summary of tuple count across all batches
- This operation is relatively lightweight but requires coordination among all parallel workers
- Essential for proper batch management decisions and memory pressure detection in parallel hash joins

## Simplified Source

```c
static void
ExecParallelHashMergeCounters(HashJoinTable hashtable)
{
    ParallelHashJoinState *pstate = hashtable->parallel_state;

    // Acquire exclusive lock to safely update shared counters
    LWLockAcquire(&pstate->lock, LW_EXCLUSIVE);

    // Reset global tuple total
    pstate->total_tuples = 0;

    // Merge local counters into shared state for each batch
    for (int i = 0; i < hashtable->nbatch; ++i)
    {
        ParallelHashJoinBatchAccessor *batch = &hashtable->batches[i];

        // Add local counters to shared counters
        batch->shared->size += batch->size;
        batch->shared->estimated_size += batch->estimated_size;
        batch->shared->ntuples += batch->ntuples;
        batch->shared->old_ntuples += batch->old_ntuples;

        // Reset local counters to zero
        batch->size = 0;
        batch->estimated_size = 0;
        batch->ntuples = 0;
        batch->old_ntuples = 0;

        // Update global total
        pstate->total_tuples += batch->shared->ntuples;
    }

    LWLockRelease(&pstate->lock);
}
```