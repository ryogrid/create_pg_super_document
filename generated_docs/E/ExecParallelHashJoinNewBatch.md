# ExecParallelHashJoinNewBatch

## Location
[src/backend/executor/nodeHashjoin.c:1172-1314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1172-L1314)

## Overview
Chooses and attaches to a new batch for processing in a parallel hash join operation, coordinating between multiple worker processes using barriers and distributed batch assignment.

## Definition
```c
static bool ExecParallelHashJoinNewBatch(HashJoinState *hjstate)
```

## Detailed Description
This function manages batch selection and coordination for parallel hash join operations. Unlike the sequential version, it must coordinate between multiple worker processes that are simultaneously working on different batches. The function uses an atomic counter-based distributor to assign different starting points to each worker, implements a state machine with barriers to synchronize batch processing phases (elect, allocate, load, probe, scan, free), and handles the complex lifecycle of parallel batch processing including hash table allocation, tuple loading, and cleanup.

The function implements a round-robin search strategy starting from different points for each worker to distribute work evenly. It uses PostgreSQL's barrier synchronization primitives to ensure proper coordination between phases of batch processing.

## Parameters / Member Variables
- `hjstate`: The HashJoinState containing the hash table and parallel execution state, including barrier and shared memory structures for coordination

## Dependencies
- Functions called/Symbols referenced:
  - [ExecHashTableDetachBatch](ExecHashTableDetachBatch.md)
  - [pg_atomic_fetch_add_u32](../p/pg_atomic_fetch_add_u32.md)
  - [BarrierAttach](../B/BarrierAttach.md)
  - [BarrierArriveAndWait](../B/BarrierArriveAndWait.md)
  - [ExecParallelHashTableAlloc](ExecParallelHashTableAlloc.md)
  - [ExecParallelHashTableSetCurrentBatch](ExecParallelHashTableSetCurrentBatch.md)
  - [sts_begin_parallel_scan](../s/sts_begin_parallel_scan.md)
  - [sts_parallel_scan_next](../s/sts_parallel_scan_next.md)
  - [ExecForceStoreMinimalTuple](ExecForceStoreMinimalTuple.md)
  - [ExecParallelHashTableInsertCurrentBatch](ExecParallelHashTableInsertCurrentBatch.md)
  - [sts_end_parallel_scan](../s/sts_end_parallel_scan.md)
  - [BarrierDetach](../B/BarrierDetach.md)
  - [BarrierPhase](../B/BarrierPhase.md)
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- Returns true if a batch was successfully selected and is ready for probing, false if no more batches remain
- Implements a complex state machine with phases: PHJ_BATCH_ELECT, PHJ_BATCH_ALLOCATE, PHJ_BATCH_LOAD, PHJ_BATCH_PROBE, PHJ_BATCH_SCAN, PHJ_BATCH_FREE
- Uses atomic operations and barriers to coordinate between parallel workers without deadlocks
- The function is static and specific to parallel hash join execution
- Critical for efficient parallel processing of large hash joins across multiple worker processes
- Each worker may participate in different phases of batch processing depending on timing and coordination

## Simplified Source

```c
static bool
ExecParallelHashJoinNewBatch(HashJoinState *hjstate)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int start_batchno, batchno;

    // Detach from current batch if attached
    if (hashtable->curbatch >= 0) {
        hashtable->batches[hashtable->curbatch].done = true;
        ExecHashTableDetachBatch(hashtable);
    }

    // Use atomic counter to distribute workers across different starting batches
    batchno = start_batchno =
        pg_atomic_fetch_add_u32(&hashtable->parallel_state->distributor, 1) %
        hashtable->nbatch;

    // Search for an available batch to process
    do {
        uint32 hashvalue;
        MinimalTuple tuple;
        TupleTableSlot *slot;

        if (!hashtable->batches[batchno].done) {
            SharedTuplestoreAccessor *inner_tuples;
            Barrier *batch_barrier = &hashtable->batches[batchno].shared->batch_barrier;

            // Participate in batch processing state machine
            switch (BarrierAttach(batch_barrier)) {
                case PHJ_BATCH_ELECT:
                    // Elect one worker to allocate hash table
                    if (BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_ELECT))
                        ExecParallelHashTableAlloc(hashtable, batchno);
                    // Fall through

                case PHJ_BATCH_ALLOCATE:
                    // Wait for hash table allocation to complete
                    BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_ALLOCATE);
                    // Fall through

                case PHJ_BATCH_LOAD:
                    // Load tuples from shared tuple store into hash table
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    inner_tuples = hashtable->batches[batchno].inner_tuples;
                    sts_begin_parallel_scan(inner_tuples);

                    while ((tuple = sts_parallel_scan_next(inner_tuples, &hashvalue))) {
                        ExecForceStoreMinimalTuple(tuple, hjstate->hj_HashTupleSlot, false);
                        slot = hjstate->hj_HashTupleSlot;
                        ExecParallelHashTableInsertCurrentBatch(hashtable, slot, hashvalue);
                    }

                    sts_end_parallel_scan(inner_tuples);
                    BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_LOAD);
                    // Fall through

                case PHJ_BATCH_PROBE:
                    // Batch ready for probing - return control to caller
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    sts_begin_parallel_scan(hashtable->batches[batchno].outer_tuples);
                    return true;

                case PHJ_BATCH_SCAN:
                    // Batch in scan phase - detach and try next batch
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    hashtable->batches[batchno].done = true;
                    ExecHashTableDetachBatch(hashtable);
                    break;

                case PHJ_BATCH_FREE:
                    // Batch already completed - detach and continue
                    BarrierDetach(batch_barrier);
                    hashtable->batches[batchno].done = true;
                    hashtable->curbatch = -1;
                    break;

                default:
                    elog(ERROR, "unexpected batch phase %d", BarrierPhase(batch_barrier));
            }
        }

        // Try next batch in round-robin fashion
        batchno = (batchno + 1) % hashtable->nbatch;
    } while (batchno != start_batchno);

    return false;  // No more batches available
}
```