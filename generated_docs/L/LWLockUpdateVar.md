# LWLockUpdateVar

## Location
[src/backend/storage/lmgr/lwlock.c:1722-1782](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1722-L1782)

## Overview
LWLockUpdateVar atomically updates a variable and wakes up all processes waiting for that variable change while holding an exclusive lock.

## Definition

```c
void
LWLockUpdateVar(LWLock *lock, pg_atomic_uint64 *valptr, uint64 val)
```
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
  - [pg_atomic_exchange_u64](../p/pg_atomic_exchange_u64.md) (atomic variable update with full barrier)
  - [LWLockWaitListLock](LWLockWaitListLock.md)/LWLockWaitListUnlock (wait queue protection)
  - proclist_foreach_modify/proclist_delete/proclist_push_tail (wait queue management)
  - GetPGProcByNumber (process lookup)
  - pg_write_barrier (memory ordering)
  - [PGSemaphoreUnlock](../P/PGSemaphoreUnlock.md) (process wakeup)
- Called from (representative examples):
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md) (WAL insertion coordination)
  - [WALInsertLockUpdateInsertingAt](../W/WALInsertLockUpdateInsertingAt.md) (WAL insertion progress updates)

## Notes and Other Information
- Caller must hold the lock in exclusive mode before calling
- The pg_atomic_exchange_u64() provides full memory barrier semantics
- Only wakes LW_WAIT_UNTIL_FREE waiters, not regular lock waiters
- Uses a two-phase approach: collect waiters while holding wait list lock, then wake them after releasing it
- Process states are carefully managed through LW_WS_WAITING -> LW_WS_PENDING_WAKEUP -> LW_WS_NOT_WAITING transitions
- Critical for WAL insertion coordination where multiple backends wait for insertion progress

## Simplified Source

```c
// Simplified version of LWLockUpdateVar
void LWLockUpdateVar(LWLock *lock, pg_atomic_uint64 *valptr, uint64 val) {
    proclist_head wakeup;
    proclist_mutable_iter iter;

    // Atomically update the variable (provides full memory barrier)
    pg_atomic_exchange_u64(valptr, val);

    // Initialize temporary wakeup list
    proclist_init(&wakeup);

    // Lock the wait list to safely examine waiters
    LWLockWaitListLock(lock);
    Assert(pg_atomic_read_u32(&lock->state) & LW_VAL_EXCLUSIVE);

    // Find all LW_WAIT_UNTIL_FREE waiters and move them to wakeup list
    proclist_foreach_modify(iter, &lock->waiters, lwWaitLink) {
        PGPROC *waiter = GetPGProcByNumber(iter.cur);

        if (waiter->lwWaitMode != LW_WAIT_UNTIL_FREE)
            break;  // LW_WAIT_UNTIL_FREE waiters are always at front

        // Move waiter from lock's queue to temporary wakeup list
        proclist_delete(&lock->waiters, iter.cur, lwWaitLink);
        proclist_push_tail(&wakeup, iter.cur, lwWaitLink);

        // Update waiter state
        Assert(waiter->lwWaiting == LW_WS_WAITING);
        waiter->lwWaiting = LW_WS_PENDING_WAKEUP;
    }

    // Release wait list lock
    LWLockWaitListUnlock(lock);

    // Wake up all waiters we collected
    proclist_foreach_modify(iter, &wakeup, lwWaitLink) {
        PGPROC *waiter = GetPGProcByNumber(iter.cur);

        proclist_delete(&wakeup, iter.cur, lwWaitLink);
        pg_write_barrier();  // Ensure state change is visible
        waiter->lwWaiting = LW_WS_NOT_WAITING;
        PGSemaphoreUnlock(waiter->sem);
    }
}
```

Key simplifications made:
- Emphasized the two-phase approach: update variable → collect waiters → wake them
- Preserved critical memory barrier semantics for correctness
- Focused on the core waiter management algorithm
- Removed detailed debug output while maintaining essential assertions