# ExecHashTableDetach

## Location
src/backend/executor/nodeHash.c: 3381 - 3430

## Overview
Detaches from all shared parallel hash join resources and performs global cleanup when the last process detaches.

## Definition


## Detailed Description
This function handles the complete detachment of a worker process from all shared parallel hash join resources. It operates at a higher level than ExecHashTableDetachBatch, managing the overall parallel hash join state rather than individual batch resources. The function ensures proper coordination between all participating processes through barrier synchronization.

The function first verifies that the parallel hash join has reached the appropriate execution phase (PHJ_BUILD_RUN or PHJ_BUILD_FREE). It then closes all temporary files associated with the hash table batches and coordinates the final cleanup. When the last process detaches, it frees the shared batch metadata structures and transitions the build barrier to PHJ_BUILD_FREE, signaling to any late-joining processes that they should give up immediately.

After detachment, the function nullifies the parallel state reference, effectively disconnecting the local hash table from the shared parallel execution context.

## Parameters / Member Variables
- : The HashJoinTable structure containing parallel state and batch information

## Dependencies
- Functions called/Symbols referenced:
  - BarrierPhase
  - BarrierArriveAndDetach
  - [sts_end_write](../s/sts_end_write.md)
  - [sts_end_parallel_scan](../s/sts_end_parallel_scan.md)
  - DsaPointerIsValid
  - [dsa_free](../d/dsa_free.md)
- Data types used:
  - [HashJoinTable](../H/HashJoinTable.md)
  - ParallelHashJoinState
- Phase constants:
  - PHJ_BUILD_RUN
  - PHJ_BUILD_FREE
- Special values:
  - InvalidDsaPointer
- Called from (representative examples):
  - ExecShutdownHashJoin
  - ExecHashJoinReInitializeDSM

## Notes and Other Information
- Only operates when parallel state exists and is in PHJ_BUILD_RUN phase
- Handles both normal completion and late-joining process scenarios
- Closes all temporary files for both inner and outer tuple storage across all batches
- The last detaching process is responsible for freeing shared batch metadata
- Sets parallel_state to NULL after detachment to prevent further parallel operations
- Uses barrier synchronization to ensure proper coordination between all worker processes
- Late-joining processes will see PHJ_BUILD_FREE state and abandon execution immediately