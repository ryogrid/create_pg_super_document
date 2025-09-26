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