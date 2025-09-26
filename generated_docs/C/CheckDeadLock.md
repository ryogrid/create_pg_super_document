# CheckDeadLock

## Location
[src/backend/storage/lmgr/proc.c:1759-1844](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L1759-L1844)

## Overview
CheckDeadLock performs deadlock detection when the deadlock timeout fires, analyzing lock dependencies and removing the current process from wait queues if a hard deadlock is detected.

## Definition
```c
static void CheckDeadLock(void)
```

## Detailed Description
CheckDeadLock is the timeout handler function that gets invoked when a process has been waiting for a lock longer than the configured deadlock_timeout. This function implements PostgreSQL's deadlock detection and resolution mechanism.

The function performs several critical operations:
1. **Global lock acquisition**: Acquires all lock partition locks in a consistent order to get a stable view of the lock system
2. **Early exit check**: Verifies the process is still waiting (it might have been awakened while acquiring locks)
3. **Deadlock detection**: Calls the main deadlock detection algorithm (DeadLockCheck)
4. **Deadlock resolution**: If a hard deadlock is found, removes the current process from the wait queue
5. **Lock cleanup**: Releases all partition locks in reverse order

The function only handles hard deadlocks by aborting the current transaction. Soft deadlocks (resolved by queue reordering) are handled by the DeadLockCheck algorithm itself.

## Parameters / Member Variables
None - operates on global state and MyProc

## Dependencies
- Functions called/Symbols referenced:
  - LockHashPartitionLockByIndex (get lock partition by index)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (acquire/release lightweight locks)
  - [DeadLockCheck](../D/DeadLockCheck.md) (main deadlock detection algorithm)
  - [DumpAllLocks](../D/DumpAllLocks.md) (debugging - [dump](../d/dump.md) all locks if LOCK_DEBUG enabled)
  - [RemoveFromWaitQueue](../R/RemoveFromWaitQueue.md) (remove process from lock wait queue)
  - [LockTagHashCode](../L/LockTagHashCode.md) (compute hash code for lock tag)
  - NUM_LOCK_PARTITIONS (total number of lock partitions)
  - DS_HARD_DEADLOCK (deadlock state indicating unresolvable deadlock)

- Called from (representative examples):
  - [ProcSleep](../P/ProcSleep.md) (when deadlock timeout fires during lock wait)

## Notes and Other Information
- This is a static function, only called internally within the process management module
- Acquires ALL lock partition locks to ensure a consistent global view during deadlock detection
- Creates a critical section that cannot be interrupted by cancel/die signals
- Handles the case where the process might have been awakened between timeout and lock acquisition
- Uses reverse-order lock release to avoid O(N²) behavior and prevent blocking other processes
- Only removes the current process from wait queues - relies on transaction abort to release other held locks
- Includes debugging support via DumpAllLocks when LOCK_DEBUG is enabled
- Located in src/backend/storage/lmgr/proc.c:1759-1844
- Part of PostgreSQL's comprehensive deadlock prevention and detection system