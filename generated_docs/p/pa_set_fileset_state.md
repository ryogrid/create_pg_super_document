# pa_set_fileset_state

## Location
src/backend/replication/logical/applyparallelworker.c: 1498 - 1517

## Overview
pa_set_fileset_state sets the fileset state for a parallel apply worker, managing the transition of serialized transaction data from leader worker to parallel workers in PostgreSQL logical replication.

## Definition


## Detailed Description
This function manages the fileset state transitions in PostgreSQL's logical replication parallel worker architecture. It provides thread-safe updates to the shared worker state, specifically controlling when and how serialized transaction data becomes available to parallel workers.

When the fileset state is set to FS_SERIALIZE_DONE, the function performs additional validation to ensure it's being called by the leader worker and copies the stream fileset from the leader worker's context to the shared memory structure. This enables parallel workers to access the serialized transaction data that was prepared by the leader.

The function uses spinlocks to ensure atomic updates to the shared state, preventing race conditions between the leader worker and parallel workers when accessing fileset information.

## Parameters / Member Variables
- : Pointer to ParallelApplyWorkerShared structure containing shared state between leader and parallel workers
- : PartialFileSetState enum value indicating the new state of the fileset (e.g., FS_SERIALIZE_DONE)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - am_leader_apply_worker
  - Assert
  - FS_SERIALIZE_DONE (enum constant)
  - MyLogicalRepWorker (global variable)
- Called from (representative examples):
  - pa_process_spooled_messages_if_required
  - pa_switch_to_partial_serialize
  - apply_handle_stream_prepare
  - apply_handle_stream_abort
  - apply_handle_stream_commit

## Notes and Other Information
- Uses spinlocks for thread-safe access to shared worker state
- Only copies the actual fileset when state is FS_SERIALIZE_DONE
- Includes assertions to verify it's called by the leader worker when copying filesets
- Part of the mechanism that coordinates data sharing between leader and parallel workers
- Critical for ensuring parallel workers can access serialized transaction data at the right time
- The fileset copying operation is protected by assertions ensuring proper calling context