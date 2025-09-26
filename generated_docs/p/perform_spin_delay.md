# perform_spin_delay

## Location
[src/backend/storage/lmgr/s_lock.c:132-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/s_lock.c#L132-L191)

## Overview
perform_spin_delay implements intelligent delay handling during spinlock contention, using exponential backoff with randomization to balance CPU efficiency and responsiveness.

## Definition
```c
void perform_spin_delay(SpinDelayStatus *status)
```

## Detailed Description
This function manages the delay strategy when a spinlock cannot be immediately acquired. It implements a sophisticated multi-stage approach:

1. **CPU-specific delay**: Executes a brief CPU-specific delay (SPIN_DELAY) each iteration
2. **Spin counting**: Tracks the number of spins and triggers blocking after spins_per_delay attempts
3. **Progressive delays**: Starts with MIN_DELAY_USEC and exponentially increases delay time
4. **Randomization**: Adds random variance (1X to 2X) to prevent thundering herd effects
5. **Stuck lock detection**: Calls s_lock_stuck() if delays exceed NUM_DELAYS threshold
6. **Wait event reporting**: Reports WAIT_EVENT_SPIN_DELAY for monitoring tools

The exponential backoff with randomization helps reduce contention while the delay wraps back to minimum when maximum is exceeded, preventing indefinite delay growth.

## Parameters / Member Variables
- `status`: Pointer to SpinDelayStatus structure tracking delay state and statistics

## Dependencies
- Functions called/Symbols referenced:
  - SPIN_DELAY (CPU-specific delay primitive)
  - s_lock_stuck (stuck lock detection and reporting)
  - pgstat_report_wait_start/pgstat_report_wait_end (wait event monitoring)
  - pg_usleep (microsecond sleep function)
  - pg_prng_double (random number generation)
  - NUM_DELAYS, MIN_DELAY_USEC, MAX_DELAY_USEC (configuration constants)
  - SpinDelayStatus (delay tracking structure)
- Called from (representative examples):
  - s_lock (main spinlock acquisition function)
  - LockBufHdr (buffer header locking)
  - WaitBufHdrUnlocked (buffer management)
  - LWLockWaitListLock (lightweight lock management)

## Notes and Other Information
- Uses exponential backoff with randomization to prevent thundering herd scenarios
- Includes wait event reporting for performance monitoring and debugging
- In S_LOCK_TEST mode, prints progress indicators to stdout
- Delay time wraps from maximum back to minimum to prevent unbounded growth
- The spins counter resets after each delay period
- Critical component of PostgreSQL's spinlock infrastructure used throughout the system
- Balances CPU efficiency (short spins) with system responsiveness (progressive delays)