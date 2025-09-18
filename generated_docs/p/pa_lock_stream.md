# pa_lock_stream

## Location
src/backend/replication/logical/applyparallelworker.c: 1540 - 1546

## Overview
pa_lock_stream is a helper function that acquires a lock for stream blocks in PostgreSQL's logical replication parallel worker system, ensuring coordinated access to streaming transaction data.

## Definition


## Detailed Description
This function provides a specialized locking mechanism for streaming transaction blocks in PostgreSQL's logical replication parallel worker architecture. It acts as a wrapper around LockApplyTransactionForSession, specifically setting the lock tag field to PARALLEL_APPLY_LOCK_STREAM to distinguish stream locks from other types of transaction locks.

The function is part of the broader locking strategy used to coordinate between leader workers and parallel workers when processing streaming transactions. Stream locks ensure that parallel workers can safely access and process serialized transaction data without conflicts, maintaining data consistency during parallel replication operations.

The lock is associated with both a transaction ID and the current subscription ID, providing fine-grained control over access to specific transaction streams within a logical replication context.

## Parameters / Member Variables
- : TransactionId of the transaction for which to acquire the stream lock
- : LOCKMODE specifying the type of lock to acquire (e.g., AccessShareLock, AccessExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - LockApplyTransactionForSession
  - PARALLEL_APPLY_LOCK_STREAM (constant)
  - MyLogicalRepWorker (global variable)
- Called from (representative examples):
  - pa_process_spooled_messages_if_required
  - pa_switch_to_partial_serialize
  - pa_decr_and_wait_stream_block
  - apply_handle_stream_stop
  - apply_handle_stream_abort

## Notes and Other Information
- Helper function that abstracts the complexity of stream-specific locking
- Uses PARALLEL_APPLY_LOCK_STREAM to distinguish from other transaction locks
- Part of the coordination mechanism between leader and parallel workers
- Ensures safe access to streaming transaction data during parallel replication
- Works in conjunction with the corresponding unlock function (pa_unlock_stream)
- Critical for maintaining data consistency in parallel logical replication scenarios
- Locks are associated with both transaction ID and subscription ID for precise control