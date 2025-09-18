# pa_decr_and_wait_stream_block

## Location
src/backend/replication/logical/applyparallelworker.c: 1591 - 1617

## Overview
Decrements the count of pending streaming blocks and waits on the stream lock when no blocks remain available for processing.

## Definition
```c
void pa_decr_and_wait_stream_block(void)
```

## Detailed Description
This function is a critical synchronization mechanism in PostgreSQL's logical replication parallel apply worker system. It manages the count of pending streaming blocks and implements a waiting mechanism when all blocks have been processed. The function first decrements the atomic counter for pending stream chunks. If the counter reaches zero (meaning no more blocks are pending), it performs a lock/unlock sequence on the stream lock, which effectively causes the worker to wait until new stream data becomes available. The function includes safety checks to ensure it's only called in valid contexts and handles the special case where spooled messages are available for processing.

## Parameters / Member Variables
This function takes no parameters and operates on shared state.

## Dependencies
- Functions called/Symbols referenced:
  - am_parallel_apply_worker
  - pg_atomic_read_u32
  - pa_has_spooled_message_pending
  - pg_atomic_sub_fetch_u32
  - pa_lock_stream
  - pa_unlock_stream
- Called from (representative examples):
  - apply_handle_stream_stop
  - apply_handle_stream_abort

## Notes and Other Information
- Only valid to call from within a parallel apply worker context (enforced by Assert)
- Uses atomic operations to safely decrement the pending_stream_count in shared memory
- The lock/unlock pattern on the stream lock is a synchronization idiom that blocks until stream data is available
- Special handling for spooled messages: if spooled messages are pending when stream count is 0, function returns early
- Error handling: throws an error if pending_stream_count is 0 without spooled messages, indicating an invalid state
- The function accesses MyParallelShared->pending_stream_count and MyParallelShared->xid
- Uses AccessShareLock mode for the stream locking mechanism
- Part of the stream processing flow control in parallel logical replication workers