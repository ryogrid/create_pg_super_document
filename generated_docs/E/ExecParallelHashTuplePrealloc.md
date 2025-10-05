# ExecParallelHashTuplePrealloc

## Location
[src/backend/executor/nodeHash.c:3541-3601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3541-L3601)

## Overview
Manages memory preallocation for hash join batches in parallel execution, tracking space usage and triggering repartitioning when memory limits are exceeded.

## Definition
```c
static bool
ExecParallelHashTuplePrealloc(HashJoinTable hashtable, int batchno, size_t size)
```

## Detailed Description
This function maintains a running estimation of memory usage for hash join batches during parallel execution. It simulates how memory chunks will be distributed among workers to predict whether a batch will fit in memory when reloaded. The function performs several critical tasks:

1. **Memory Tracking**: Updates the estimated memory size for the specified batch
2. **Growth Detection**: Checks if other workers have requested table growth (more batches/buckets)
3. **Space Management**: Determines if the batch would exceed available memory and triggers repartitioning
4. **Coordination**: Uses locks to coordinate decision-making among parallel workers

The estimation is approximate because actual tuple packing differs between the preallocation phase (where all workers collaborate) and the reload phase (where fewer workers may participate). The function deliberately overestimates to avoid memory exhaustion.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable being operated on
- `batchno`: The batch number to preallocate for (must be > 0 and < nbatch)
- `size`: The amount of memory to preallocate (must be MAXALIGN aligned)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md)
  - [ExecParallelHashIncreaseNumBuckets](ExecParallelHashIncreaseNumBuckets.md)
  - Max (macro)
  - MAXALIGN (macro)
  - HASH_CHUNK_SIZE/HASH_CHUNK_HEADER_SIZE (constants)
  - PHJ_GROWTH_* constants
  - [HashJoinTable](../H/HashJoinTable.md), ParallelHashJoinState, ParallelHashJoinBatchAccessor (types)
- Called from:
  - [ExecParallelHashTableInsert](ExecParallelHashTableInsert.md)

## Notes and Other Information
- Returns false if the number of batches or buckets has changed, indicating the caller should reconsider tuple placement
- Uses exclusive locking to ensure consistent state updates across parallel workers
- The estimation tends to overestimate by a fraction of a chunk per worker, bounded by the number of participants
- Critical for preventing out-of-memory conditions in parallel hash joins with large datasets
- Part of PostgreSQL's adaptive hash join strategy that dynamically adjusts partitioning based on memory pressure
- Located in src/backend/executor/nodeHash.c:3541-3601

## Simplified Source

```c
static bool
ExecParallelHashTuplePrealloc(HashJoinTable hashtable, int batchno, size_t size)
{
    ParallelHashJoinState *pstate = hashtable->parallel_state;
    ParallelHashJoinBatchAccessor *batch = &hashtable->batches[batchno];
    size_t want = Max(size, HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE);

    LWLockAcquire(&pstate->lock, LW_EXCLUSIVE);

    // Check if other workers commanded us to help with growth
    if (pstate->growth == PHJ_GROWTH_NEED_MORE_BATCHES ||
        pstate->growth == PHJ_GROWTH_NEED_MORE_BUCKETS)
    {
        ParallelHashGrowth growth = pstate->growth;
        LWLockRelease(&pstate->lock);

        // Help with the requested growth operation
        if (growth == PHJ_GROWTH_NEED_MORE_BATCHES)
            ExecParallelHashIncreaseNumBatches(hashtable);
        else if (growth == PHJ_GROWTH_NEED_MORE_BUCKETS)
            ExecParallelHashIncreaseNumBuckets(hashtable);

        return false; // Caller should reconsider batch assignment
    }

    // Check if this batch would exceed memory limit
    if (pstate->growth != PHJ_GROWTH_DISABLED &&
        batch->at_least_one_chunk &&
        (batch->shared->estimated_size + want + HASH_CHUNK_HEADER_SIZE >
         pstate->space_allowed))
    {
        // This batch would be too large - trigger repartitioning
        batch->shared->space_exhausted = true;
        pstate->growth = PHJ_GROWTH_NEED_MORE_BATCHES;
        LWLockRelease(&pstate->lock);
        return false;
    }

    // Update preallocation tracking
    batch->at_least_one_chunk = true;
    batch->shared->estimated_size += want + HASH_CHUNK_HEADER_SIZE;
    batch->preallocated = want;

    LWLockRelease(&pstate->lock);
    return true;
}
```