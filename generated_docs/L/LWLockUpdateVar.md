# LWLockUpdateVar

## Location
src/backend/storage/lmgr/lwlock.c: 1722 - 1782

## Overview
LWLockUpdateVar atomically updates a variable and wakes up all processes waiting for that variable change while holding an exclusive lock.

## Definition


## Detailed Description
This function provides atomic coordination between variable updates and process notification in PostgreSQL's WAL insertion system. It first atomically sets the variable to the new value, then wakes up all processes that are waiting for variable changes via LWLockWaitForVar().

The function operates in two phases: first, it updates the variable using pg_atomic_exchange_u64() which provides a full memory barrier, guaranteeing that the variable update is visible before any waiters are awakened. Second, it scans the lock's wait queue for LW_WAIT_UNTIL_FREE waiters and moves them to a temporary wakeup list before releasing them.

The caller must hold the lock in exclusive mode when calling this function. The function carefully manages the wait queue by acquiring the wait list lock, moving waiters to a temporary list, then releasing the wait list lock before actually waking the processes.

## Parameters / Member Variables  
- : The LWLock currently held in exclusive mode
- : Pointer to the atomic uint64 variable to update
- : New value to set in the variable

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_exchange_u64 (atomic variable update with full barrier)
  - LWLockWaitListLock/LWLockWaitListUnlock (wait queue protection)
  - proclist_foreach_modify/proclist_delete/proclist_push_tail (wait queue management)
  - GetPGProcByNumber (process lookup)
  - pg_write_barrier (memory ordering)
  - PGSemaphoreUnlock (process wakeup)
- Called from (representative examples):
  - WALInsertLockAcquireExclusive (WAL insertion coordination)
  - WALInsertLockUpdateInsertingAt (WAL insertion progress updates)

## Notes and Other Information
- Caller must hold the lock in exclusive mode before calling
- The pg_atomic_exchange_u64() provides full memory barrier semantics
- Only wakes LW_WAIT_UNTIL_FREE waiters, not regular lock waiters
- Uses a two-phase approach: collect waiters while holding wait list lock, then wake them after releasing it
- Process states are carefully managed through LW_WS_WAITING -> LW_WS_PENDING_WAKEUP -> LW_WS_NOT_WAITING transitions
- Critical for WAL insertion coordination where multiple backends wait for insertion progress