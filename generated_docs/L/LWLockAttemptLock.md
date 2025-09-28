# LWLockAttemptLock

## Location
[src/backend/storage/lmgr/lwlock.c:786-856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L786-L856)

## Overview
Internal function that attempts to atomically acquire an LWLock in the specified mode without blocking, using compare-and-swap operations for thread safety.

## Definition
```c
static bool LWLockAttemptLock(LWLock *lock, LWLockMode mode)
```

## Detailed Description
LWLockAttemptLock is a critical low-level function that implements the core atomic lock acquisition logic for PostgreSQL's lightweight locks. The function:

1. **Non-blocking operation**: Attempts to acquire the lock without waiting, making it suitable for conditional locking scenarios and as a building block for higher-level locking functions.

2. **Atomic state management**: Uses atomic compare-and-exchange operations to safely modify the lock state in a multi-threaded environment, ensuring race-condition-free lock acquisition.

3. **Mode-specific logic**: 
   - For **exclusive locks**: Checks if no other locks are held (LW_LOCK_MASK == 0) and attempts to set the exclusive bit
   - For **shared locks**: Checks if no exclusive lock is held (LW_VAL_EXCLUSIVE == 0) and attempts to increment the shared counter

4. **Memory barrier semantics**: The compare-and-exchange operation serves as both the atomic update mechanism and a memory barrier, ensuring proper ordering of memory operations.

5. **Retry loop**: Continuously attempts the operation until either successful acquisition or definitive failure, handling concurrent modifications by other processes.

## Parameters / Member Variables
- `lock`: Pointer to the LWLock structure to attempt to acquire
- `mode`: The desired lock mode, either LW_EXCLUSIVE or LW_SHARED

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md) (atomic read operation)
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md) (atomic compare-and-exchange)
  - LW_EXCLUSIVE, LW_SHARED (lock mode constants)
  - LW_LOCK_MASK, LW_VAL_EXCLUSIVE, LW_VAL_SHARED (state bit masks and values)
  - Assert (parameter validation macro)
  - pg_unreachable (unreachable code marker)
  - MyProc (current process identifier, used in debug builds)

- Called from (representative examples):
  - [LWLockAcquire](LWLockAcquire.md) (blocking lock acquisition)
  - [LWLockConditionalAcquire](LWLockConditionalAcquire.md) (non-blocking conditional acquisition)
  - [LWLockAcquireOrWait](LWLockAcquireOrWait.md) (acquire or wait for lock availability)

## Notes and Other Information
- **Static function**: Only accessible within lwlock.c, serving as an internal implementation detail
- **Return semantics**: Returns false if lock was successfully acquired, true if lock is held by another process and waiting is required
- **Debug support**: In LOCK_DEBUG builds, sets the lock owner field for exclusive locks to aid in debugging
- **Performance optimization**: Always performs the atomic swap operation (even when lock appears busy) to provide memory barrier semantics, based on benchmark results
- **Thread safety**: Designed to be safe for concurrent access from multiple processes/threads
- **Lock-free design**: Uses atomic operations instead of higher-level synchronization primitives to avoid deadlocks and improve performance

## Simplified Source

```c
// Simplified version of LWLockAttemptLock
static bool
LWLockAttemptLock(LWLock *lock, LWLockMode mode)
{
    uint32 old_state;

    // Validate input parameters
    Assert(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    // Read current lock state
    old_state = pg_atomic_read_u32(&lock->state);

    // Retry loop until we determine if we can acquire the lock
    while (true)
    {
        uint32 desired_state = old_state;
        bool lock_free;

        // Check if lock is available based on requested mode
        if (mode == LW_EXCLUSIVE)
        {
            // For exclusive lock: no other locks should be held
            lock_free = (old_state & LW_LOCK_MASK) == 0;
            if (lock_free)
                desired_state += LW_VAL_EXCLUSIVE;
        }
        else
        {
            // For shared lock: no exclusive lock should be held
            lock_free = (old_state & LW_VAL_EXCLUSIVE) == 0;
            if (lock_free)
                desired_state += LW_VAL_SHARED;
        }

        // Atomic compare-and-swap operation (also serves as memory barrier)
        if (pg_atomic_compare_exchange_u32(&lock->state, &old_state, desired_state))
        {
            if (lock_free)
            {
                // Successfully acquired the lock
                return false;
            }
            else
            {
                // Lock is held by someone else
                return true;
            }
        }
        // If compare-exchange failed, old_state is updated with current value
        // Continue retry loop
    }
}
```

Key simplifications made:
- Removed detailed comments about memory barrier rationale to focus on core logic
- Simplified variable declarations and initialization
- Removed debug-specific code (LOCK_DEBUG owner assignment)
- Consolidated the return logic for clearer flow
- Removed unreachable code marker as it's not part of main logic
- Added concise comments explaining the key steps
- Maintained essential atomic operations and retry logic