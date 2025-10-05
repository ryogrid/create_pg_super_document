# s_lock

## Location
[src/backend/storage/lmgr/s_lock.c:99-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/s_lock.c#L99-L116)

## Overview
s_lock is the platform-independent function for acquiring a spinlock, implementing a busy-wait loop with intelligent delay handling to avoid CPU waste and detect stuck locks.

## Definition
```c
int s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
```

## Detailed Description
This function implements the core spinlock acquisition logic in PostgreSQL. It uses a test-and-set loop (TAS_SPIN) to repeatedly attempt lock acquisition while the lock is held by another process. The function incorporates sophisticated delay handling through SpinDelayStatus to:

- Start with short delays to handle brief contention efficiently
- Progressively increase delay intervals for longer waits
- Track statistics about delay behavior
- Detect and report stuck spinlocks that may indicate deadlocks

The function returns the total number of delays encountered, which can be used for performance monitoring and debugging.

## Parameters / Member Variables
- `lock`: Pointer to the volatile spinlock variable to acquire
- `file`: Source file name for debugging/error reporting
- `line`: Line number for debugging/error reporting  
- `func`: Function name for debugging/error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [init_spin_delay](../i/init_spin_delay.md) (initializes delay tracking)
  - TAS_SPIN (test-and-set spinlock primitive)
  - [perform_spin_delay](../p/perform_spin_delay.md) (handles delay logic and stuck lock detection)
  - [finish_spin_delay](../f/finish_spin_delay.md) (cleanup and statistics collection)
  - SpinDelayStatus (delay state management structure)
  - [slock_t](slock_t.md) (spinlock data type)
- Called from (representative examples):
  - S_LOCK (header macro)
  - [main](../m/main.md) (in test mode)
  - [test_lock_struct](../t/test_lock_struct.md)

## Notes and Other Information
- Returns the number of delay cycles encountered during acquisition
- The busy-wait loop continues until the lock is successfully acquired
- Delay handling prevents excessive CPU usage during contention
- Debug information (file, line, func) is used for error reporting if the lock becomes stuck
- Part of PostgreSQL's low-level synchronization infrastructure used throughout the system

## Simplified Source

```c
int s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
    SpinDelayStatus delayStatus;

    // Initialize delay tracking for this lock attempt
    init_spin_delay(&delayStatus, file, line, func);

    // Busy-wait loop: keep trying until lock is acquired
    while (TAS_SPIN(lock)) {
        // Handle delay and check for stuck locks
        perform_spin_delay(&delayStatus);
    }

    // Cleanup and collect statistics
    finish_spin_delay(&delayStatus);

    return delayStatus.delays;  // Return number of delays encountered
}
```