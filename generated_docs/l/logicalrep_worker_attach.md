# logicalrep_worker_attach

## Location
src/backend/replication/logical/launcher.c: 720 - 756

## Overview
Attaches the current process to a specified logical replication worker slot, establishing the connection between the process and the shared memory worker state.

## Definition


## Detailed Description
This function is responsible for safely attaching a logical replication worker process to a designated slot in the shared memory worker array. It performs critical validation to ensure the slot is available and not already in use by another process. The function operates under exclusive locking to prevent race conditions during the attachment process.

The attachment process involves:
1. Acquiring exclusive access to the logical replication worker shared memory
2. Validating the slot number is within valid bounds
3. Checking that the slot is marked as in use (allocated by the launcher)
4. Ensuring no other process is already attached to this slot
5. Setting the current process (MyProc) as the owner of the slot
6. Registering an exit handler to clean up on process termination

## Parameters / Member Variables
- : The index of the logical replication worker slot to attach to (must be between 0 and max_logical_replication_workers-1)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (LogicalRepWorkerLock, LW_EXCLUSIVE)
  - LWLockRelease
  - Assert
  - ereport/ERROR
  - before_shmem_exit
  - logicalrep_worker_onexit
- Called from (representative examples):
  - ParallelApplyWorkerMain (src/backend/replication/logical/applyparallelworker.c:914)
  - SetupApplyOrSyncWorker (src/backend/replication/logical/worker.c:4694)

## Notes and Other Information
- The function uses exclusive locking (LogicalRepWorkerLock) to ensure thread-safe access to shared worker state
- Multiple error conditions are checked: invalid slot state, slot already occupied by another process
- Upon successful attachment, an exit handler (logicalrep_worker_onexit) is registered to ensure proper cleanup
- The global variable MyLogicalRepWorker is set to point to the attached slot
- This function is typically called during the initialization phase of logical replication worker processes