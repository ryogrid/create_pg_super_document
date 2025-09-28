# LWLockWaitListLock

## Location
[src/backend/storage/lmgr/lwlock.c:857-908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L857-L908)

## Overview
Acquires a spinlock on an LWLock's wait list to enable safe concurrent manipulation of waiting processes while allowing non-conflicting lock operations to continue.

## Definition
```c
static void LWLockWaitListLock(LWLock *lock)
```

## Detailed Description
LWLockWaitListLock is a specialized synchronization function that protects the wait list of an LWLock from concurrent modifications. This function implements a critical design pattern in PostgreSQL's locking system:

1. **Wait list protection**: Ensures that operations on the wait list (adding/removing waiting processes) are atomic and safe from race conditions.

2. **Non-blocking for regular lock operations**: While the wait list is locked, processes can still acquire and release the actual LWLock if there are no conflicts, maintaining high concurrency.

3. **Spin-based implementation**: Uses atomic operations combined with spin-waiting to minimize the overhead of acquiring the wait list lock, which should be held for very short periods.

4. **Two-phase acquisition strategy**:
   - **Phase 1**: Attempts direct acquisition using atomic fetch-or operation
   - **Phase 2**: If unsuccessful, enters a spin-wait loop using adaptive delay strategies to avoid excessive CPU usage

5. **Performance optimization**: Uses spin delay mechanisms with backoff to balance responsiveness with CPU efficiency during contention.

## Parameters / Member Variables
- `lock`: Pointer to the LWLock whose wait list needs to be locked for manipulation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md) (atomic fetch-and-or operation)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md) (atomic read operation)  
  - LW_FLAG_LOCKED (bit flag indicating wait list lock status)
  - SpinDelayStatus (structure for managing spin delay behavior)
  - init_local_spin_delay, perform_spin_delay, finish_spin_delay (spin delay management functions)
  - [get_lwlock_stats_entry](../g/get_lwlock_stats_entry.md) (statistics collection, if LWLOCK_STATS enabled)
  - [lwlock_stats](../l/lwlock_stats.md) (statistics structure, if LWLOCK_STATS enabled)

- Called from (representative examples):
  - [LWLockWakeup](LWLockWakeup.md) (waking up waiting processes)
  - [LWLockQueueSelf](LWLockQueueSelf.md) (adding current process to wait queue)
  - [LWLockDequeueSelf](LWLockDequeueSelf.md) (removing current process from wait queue)
  - [LWLockUpdateVar](LWLockUpdateVar.md) (updating lock variables atomically)

## Notes and Other Information
- **Critical timing requirement**: Comments emphasize that the mutex should be held for only very short periods to maintain system performance
- **Statistics collection**: When compiled with LWLOCK_STATS, tracks spin delay counts for performance analysis
- **Adaptive spinning**: Uses PostgreSQL's spin delay framework that adapts delay patterns based on system behavior and contention levels
- **Design philosophy**: Separates wait list management from actual lock acquisition/release, allowing for more granular control and better concurrency
- **Memory ordering**: Atomic operations provide necessary memory barriers to ensure proper synchronization across processes
- **No return value**: Function always succeeds in acquiring the wait list lock, blocking until successful

## Simplified Source

```c
// Simplified version of LWLockWaitListLock
static void LWLockWaitListLock(LWLock *lock) {
    uint32 old_state;

    while (true) {
        // Try to acquire the wait list lock atomically
        old_state = pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_LOCKED);
        if (!(old_state & LW_FLAG_LOCKED)) {
            break;  // Successfully acquired lock
        }

        // Spin-wait until lock becomes available
        SpinDelayStatus delayStatus;
        init_local_spin_delay(&delayStatus);

        while (old_state & LW_FLAG_LOCKED) {
            perform_spin_delay(&delayStatus);  // Adaptive delay
            old_state = pg_atomic_read_u32(&lock->state);
        }

        finish_spin_delay(&delayStatus);

        // Retry acquisition (lock might be re-acquired by another process)
    }
}
```

Key simplifications made:
- Removed LWLOCK_STATS conditional compilation and statistics tracking
- Simplified spin delay logic while preserving the adaptive delay mechanism
- Removed detailed comments about timing and performance considerations
- Consolidated the core algorithm into clear, readable steps
- Maintained the essential two-phase acquisition strategy (direct attempt + spin-wait)