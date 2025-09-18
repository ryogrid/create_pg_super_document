# ConditionalXactLockTableWait

## Location
src/backend/storage/lmgr/lmgr.c: 733 - 777

## Overview
ConditionalXactLockTableWait attempts to wait for a specified transaction to complete, but returns immediately if the lock cannot be acquired without blocking, providing a non-blocking alternative to XactLockTableWait.

## Definition
```c
bool ConditionalXactLockTableWait(TransactionId xid)
```

## Detailed Description
ConditionalXactLockTableWait is the non-blocking variant of XactLockTableWait that provides the same transaction synchronization functionality but with a crucial difference: it will not block if the target transaction is still running. Instead, it immediately returns false if the ShareLock on the transaction cannot be acquired without waiting.

The function follows the same logic as XactLockTableWait for handling subtransactions - it automatically traverses up the transaction hierarchy to find and wait on the topmost parent transaction. However, it uses the 'dontWait' parameter of LockAcquire set to true, which causes the function to return LOCKACQUIRE_NOT_AVAIL immediately if the lock would require blocking.

This makes it particularly useful in scenarios where the calling code needs to make decisions based on whether a transaction has completed, but cannot afford to block indefinitely. It's commonly used in tuple locking operations where alternative strategies may be available if the transaction is still running.

## Parameters / Member Variables
- `xid`: The TransactionId to conditionally wait for completion

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_TRANSACTION (constructs transaction-specific lock tag)
  - LockAcquire (attempts to acquire ShareLock with dontWait=true)
  - LockRelease (releases the ShareLock if acquired)
  - TransactionIdIsInProgress (checks if transaction is still running)
  - SubTransGetTopmostTransaction (finds topmost parent transaction)
  - GetTopTransactionIdIfAny (gets current transaction's XID for comparison)
  - pg_usleep (sleep function for retry logic)
  - LOCKACQUIRE_NOT_AVAIL (constant indicating lock unavailable)
- Called from (representative examples):
  - heap_lock_tuple (heap tuple locking with fallback strategies)
  - heapam_tuple_lock (heap access method tuple locking)
  - Do_MultiXactIdWait (multi-transaction waiting logic)

## Notes and Other Information
- Returns true if the transaction has completed (lock acquired and released), false if still running
- Non-blocking behavior makes it suitable for optimistic locking strategies and timeout implementations
- Includes the same subtransaction hierarchy traversal logic as XactLockTableWait
- Uses the same retry mechanism with pg_usleep(1000L) for ProcArray/lock table synchronization edge cases
- No error context callback mechanism since it's designed for conditional/optimistic usage patterns
- Essential for implementing NOWAIT-style operations and avoiding deadlocks in complex locking scenarios
- Simpler parameter interface than XactLockTableWait since it's focused on the conditional aspect rather than detailed error reporting
- Commonly used in tuple locking where alternative locking modes or strategies may be employed if the primary transaction is still active