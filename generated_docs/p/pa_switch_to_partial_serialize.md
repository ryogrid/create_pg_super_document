# pa_switch_to_partial_serialize

## Location
src/backend/replication/logical/applyparallelworker.c: 1211 - 1243

## Overview
Switches a parallel apply worker transaction to partial serialize mode, where remaining transaction data is serialized to a file to prevent deadlocks between parallel workers.

## Definition
```c
void pa_switch_to_partial_serialize(ParallelApplyWorkerInfo *winfo, bool stream_locked)
```

## Detailed Description
pa_switch_to_partial_serialize implements a deadlock avoidance mechanism in PostgreSQL logical replication by transitioning a parallel apply worker from direct data transmission to file-based serialization. When a parallel worker becomes stuck or potentially deadlocked, this function stops sending data directly to the worker and instead begins serializing the remaining transaction changes to a file. The function initializes the stream fileset for the transaction, acquires necessary locks to coordinate with the parallel worker, and updates the fileset state to indicate serialization is in progress. This approach allows the system to continue processing while avoiding potential deadlocks between parallel apply workers.

## Parameters / Member Variables
- `winfo`: ParallelApplyWorkerInfo structure containing information about the parallel worker and shared state
- `stream_locked`: Boolean indicating whether the stream lock is already held, affecting whether additional locking is needed

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelApplyWorkerInfo](../P/ParallelApplyWorkerInfo.md)
  - [stream_start_internal](../s/stream_start_internal.md)
  - [pa_lock_stream](pa_lock_stream.md)
  - AccessExclusiveLock
  - [pa_set_fileset_state](pa_set_fileset_state.md)
  - FS_SERIALIZE_IN_PROGRESS
  - ereport
  - LOG
  - [errmsg](../e/errmsg.md)

- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md)
  - [apply_handle_stream_stop](../a/apply_handle_stream_stop.md)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md)

## Notes and Other Information
- Logs a message indicating the switch to serialization mode for debugging and monitoring purposes
- Sets the serialize_changes flag to true in the worker info structure to indicate the mode change
- Uses AccessExclusiveLock when acquiring the stream lock to ensure exclusive access
- The fileset state is updated to FS_SERIALIZE_IN_PROGRESS to coordinate with the parallel worker
- Designed to handle situations where parallel workers might become stuck waiting on locks from other backends
- Part of the broader deadlock prevention strategy for parallel logical replication