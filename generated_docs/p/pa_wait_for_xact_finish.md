# pa_wait_for_xact_finish

## Location
src/backend/replication/logical/applyparallelworker.c: 1274 - 1306

## Overview
Waits for a parallel apply workers transaction to completely finish, implementing deadlock detection and proper synchronization between leader and parallel workers.

## Definition
```c
static void pa_wait_for_xact_finish(ParallelApplyWorkerInfo *winfo)
```

## Detailed Description
pa_wait_for_xact_finish ensures proper transaction coordination and deadlock detection between the leader and parallel apply workers in PostgreSQL logical replication. The function operates in three phases: first, it waits for the parallel worker to reach the PARALLEL_TRANS_STARTED state, ensuring the worker has acquired the transaction lock before the leader proceeds. Second, it attempts to acquire and immediately release a shared lock on the same transaction, which serves as a deadlock detection mechanism - if the parallel worker is stuck, this operation will detect the deadlock. Finally, it verifies that the worker has reached the PARALLEL_TRANS_FINISHED state, throwing an error if the worker failed during transaction processing.

## Parameters / Member Variables
- `winfo`: ParallelApplyWorkerInfo structure containing information about the parallel worker and its shared transaction state

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelApplyWorkerInfo](../P/ParallelApplyWorkerInfo.md)
  - [pa_wait_for_xact_state](pa_wait_for_xact_state.md)
  - PARALLEL_TRANS_STARTED
  - [pa_lock_transaction](pa_lock_transaction.md)
  - [pa_unlock_transaction](pa_unlock_transaction.md)
  - [pa_get_xact_state](pa_get_xact_state.md)
  - PARALLEL_TRANS_FINISHED
  - AccessShareLock
  - ereport
  - ERROR
  - [errcode](../e/errcode.md)
  - ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
  - [errmsg](../e/errmsg.md)

- Called from (representative examples):
  - [pa_xact_finish](pa_xact_finish.md)

## Notes and Other Information
- Implements a two-phase waiting strategy to prevent race conditions between leader and parallel workers
- Uses transaction locking as a deadlock detection mechanism - the leader can detect if a parallel worker is stuck
- The AccessShareLock acquisition/release pattern is used purely for synchronization, not for actual data protection
- Validates transaction completion by checking the final state, providing error reporting if the worker failed unexpectedly
- Static function indicating its an internal coordination mechanism within the parallel apply system
- Critical for maintaining transaction consistency and detecting worker failures in parallel logical replication
- The immediate unlock after lock acquisition indicates this is purely a synchronization point, not resource protection