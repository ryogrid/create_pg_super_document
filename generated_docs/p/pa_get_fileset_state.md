# pa_get_fileset_state

## Location
src/backend/replication/logical/applyparallelworker.c: 1518 - 1539

## Overview
pa_get_fileset_state retrieves the current fileset state for a parallel apply worker, providing thread-safe access to shared state information in PostgreSQL logical replication.

## Definition


## Detailed Description
This static function provides a thread-safe mechanism for parallel apply workers to query the current state of the fileset in PostgreSQL's logical replication system. It accesses the shared memory structure containing the fileset state and returns the current value while ensuring atomicity through spinlock protection.

The function includes an assertion to verify it's being called from within a parallel apply worker context, ensuring proper usage within the logical replication architecture. It serves as the counterpart to pa_set_fileset_state, providing read access to the fileset state that was previously set by the leader worker.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - [am_parallel_apply_worker](../a/am_parallel_apply_worker.md)
  - SpinLockAcquire
  - SpinLockRelease  
  - MyParallelShared (global variable)
  - PartialFileSetState (enum type)
- Called from (representative examples):
  - [pa_has_spooled_message_pending](pa_has_spooled_message_pending.md)
  - [pa_process_spooled_messages_if_required](pa_process_spooled_messages_if_required.md)

## Notes and Other Information
- Static function only accessible within the applyparallelworker.c module
- Uses spinlocks for thread-safe access to shared worker state
- Includes assertion to ensure it's called from parallel worker context only
- Part of the coordination mechanism between leader and parallel workers
- Enables parallel workers to determine when serialized data becomes available
- Returns PartialFileSetState enum value indicating current fileset status
- Essential for coordinating the processing of spooled messages in parallel workers