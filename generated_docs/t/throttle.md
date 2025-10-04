# throttle

## Location
[src/backend/backup/basebackup_throttle.c:134-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_throttle.c#L134-L199)

## Overview
Core throttling function that controls data transfer rate by incrementing a counter and sleeping when necessary to comply with the configured bandwidth limits.

## Definition

```c
static void
throttle(bbsink_throttle *sink, size_t increment)
```
## Detailed Description
The  function implements the core bandwidth throttling mechanism for basebackup operations. It maintains a running counter of transferred data and enforces rate limiting by introducing deliberate delays when the transfer rate exceeds configured limits.

The function operates using a sample-based approach where it accumulates transferred bytes in a counter. Once the counter reaches the throttling sample size, it calculates the minimum time that should have elapsed for that amount of data at the configured rate. If the actual elapsed time is less than the required minimum, the function enters a sleep loop using PostgreSQL's latch mechanism.

The sleep loop is designed to handle interruptions and ensures that sufficient time passes to maintain the desired transfer rate. The function uses  with timeout to implement interruptible sleeping, allowing for clean shutdown and signal handling during the throttling process.

## Parameters / Member Variables
- `*sink`: Pointer to the bbsink_throttle structure containing throttling state and configuration
- `increment`: Number of bytes to add to the throttling counter (typically the size of data just transferred)
## Dependencies
- Functions called/Symbols referenced:
  -  (gets current time for elapsed time calculations)
  -  (resets the process latch before sleeping)
  -  (implements interruptible sleep with timeout)
  -  (handles signals and shutdown requests)
  -  (time interval type)
  - , ,  (latch wait flags)
- Called from (representative examples):
  -  (src/backend/backup/basebackup_throttle.c:112)
  -  (src/backend/backup/basebackup_throttle.c:123)

## Notes and Other Information
- This is a static function used only within the basebackup_throttle.c module
- Uses integer arithmetic with remainder operations to handle partial throttling samples
- The sleep time calculation ensures it never exceeds safe casting limits to long
- Handles concurrent WAL activity that might set latches repeatedly
- Updates the throttled_last timestamp after each throttling cycle for future calculations
- The throttling counter is reset to the remainder after processing whole samples
- Uses PostgreSQL's latch mechanism for interruptible sleeping during rate limiting

## Simplified Source

```c
static void throttle(bbsink_throttle *sink, size_t increment) {
    TimeOffset elapsed_min;

    // Accumulate transferred bytes
    sink->throttling_counter += increment;
    if (sink->throttling_counter < sink->throttling_sample)
        return;

    // Calculate minimum time that should have elapsed
    elapsed_min = sink->elapsed_min_unit *
        (sink->throttling_counter / sink->throttling_sample);

    // Sleep loop to enforce rate limit
    for (;;) {
        TimeOffset elapsed, sleep;
        int wait_result;

        // Check how much time has actually elapsed
        elapsed = GetCurrentTimestamp() - sink->throttled_last;

        // Calculate required sleep time
        sleep = elapsed_min - elapsed;
        if (sleep <= 0)
            break; // Transfer rate is acceptable

        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

        // Sleep with timeout, allowing interruption
        wait_result = WaitLatch(MyLatch,
                               WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                               (long) (sleep / 1000),
                               WAIT_EVENT_BASE_BACKUP_THROTTLE);

        if (wait_result & WL_LATCH_SET)
            CHECK_FOR_INTERRUPTS();

        if (wait_result & WL_TIMEOUT)
            break;
    }

    // Reset counter to remainder and update timestamp
    sink->throttling_counter %= sink->throttling_sample;
    sink->throttled_last = GetCurrentTimestamp();
}
```