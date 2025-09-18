# ExecHashTableDetachBatch

## Location
[src/backend/executor/nodeHash.c:3289-3380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3289-L3380)

## Overview
Detaches the current process from a shared hash join batch and performs cleanup when the last process detaches.

## Definition


## Detailed Description
This function manages the detachment of a worker process from a parallel hash join batch. It handles the complex synchronization required for parallel hash joins by using barrier synchronization primitives. When detaching, the function ensures proper cleanup of temporary files and coordinates the transition through batch processing phases (PHJ_BATCH_PROBE, PHJ_BATCH_SCAN, PHJ_BATCH_FREE).

The function implements special logic for handling early abandonment of batches when the query plan doesn't need more tuples. In such cases, it sets a flag to skip unmatched tuple processing in full/right outer joins to maintain correctness. The last process to detach is responsible for freeing shared memory resources including hash table chunks and bucket arrays.

The function also tracks peak memory usage statistics that can be reported by EXPLAIN for performance analysis.

## Parameters / Member Variables
- : The HashJoinTable structure containing the current batch information and parallel state

## Dependencies
- Functions called/Symbols referenced:
  - [sts_end_parallel_scan](../s/sts_end_parallel_scan.md)
  - BarrierPhase
  - BarrierArriveAndDetachExceptLast
  - BarrierArriveAndDetach
  - DsaPointerIsValid
  - [dsa_get_address](../d/dsa_get_address.md)
  - [dsa_free](../d/dsa_free.md)
  - Max
- Data types used:
  - [HashJoinTable](../H/HashJoinTable.md)
  - ParallelHashJoinBatch
  - HashMemoryChunk
  - dsa_pointer
  - dsa_pointer_atomic
- Phase constants:
  - PHJ_BATCH_PROBE
  - PHJ_BATCH_SCAN
  - PHJ_BATCH_FREE
- Called from (representative examples):
  - [ExecParallelPrepHashTableForUnmatched](ExecParallelPrepHashTableForUnmatched.md)
  - [ExecParallelHashJoinNewBatch](ExecParallelHashJoinNewBatch.md)
  - ExecShutdownHashJoin
  - ExecHashJoinReInitializeDSM

## Notes and Other Information
- Only operates when attached to a parallel hash join batch (curbatch >= 0)
- Uses barrier synchronization to coordinate multiple worker processes
- Handles early query termination gracefully by setting skip_unmatched flag
- The last detaching process is responsible for freeing shared memory resources
- Tracks peak memory usage for performance reporting
- Maintains phase transition invariants for parallel hash join execution
- Closes temporary files for both inner and outer tuple storage