# s_lock

## Location
src/backend/storage/lmgr/s_lock.c: 99 - 116

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
  - init_spin_delay (initializes delay tracking)
  - TAS_SPIN (test-and-set spinlock primitive)
  - perform_spin_delay (handles delay logic and stuck lock detection)
  - finish_spin_delay (cleanup and statistics collection)
  - SpinDelayStatus (delay state management structure)
  - slock_t (spinlock data type)
- Called from (representative examples):
  - S_LOCK (header macro)
  - main (in test mode)
  - test_lock_struct

## Notes and Other Information
- Returns the number of delay cycles encountered during acquisition
- The busy-wait loop continues until the lock is successfully acquired
- Delay handling prevents excessive CPU usage during contention
- Debug information (file, line, func) is used for error reporting if the lock becomes stuck
- Part of PostgreSQL's low-level synchronization infrastructure used throughout the system