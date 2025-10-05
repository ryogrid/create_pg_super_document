# launcher_determine_sleep

## Location
[src/backend/postmaster/autovacuum.c:792-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L792-L875)

## Overview
Calculates the optimal sleep duration for the autovacuum launcher based on scheduled database maintenance tasks and system constraints.

## Definition
static void launcher_determine_sleep(bool canlaunch, bool recursing, struct timeval *nap)

## Detailed Description
This function determines how long the autovacuum launcher should sleep before checking for work again. It considers multiple factors including the availability of worker processes, the next scheduled database vacuum time, and various timing constraints. The function implements intelligent sleep logic that prevents excessive CPU usage while ensuring timely response to maintenance needs. If no workers are available, it enforces a longer sleep period. When databases need immediate attention (time in the past), it rebuilds the database list to rebalance scheduling.

## Parameters / Member Variables
- : Boolean indicating whether a new autovacuum worker can be started immediately (typically based on worker availability)
- : Boolean flag to prevent infinite recursion when rebuilding the database list
- : Pointer to timeval structure where the calculated sleep time will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - dlist_tail_element
  - [TimestampDifference](../T/TimestampDifference.md)
  - [rebuild_database_list](../r/rebuild_database_list.md)
- Global variables accessed:
  - DatabaseList
  - autovacuum_naptime
  - MIN_AUTOVAC_SLEEPTIME
  - MAX_AUTOVAC_SLEEPTIME
- Called from:
  - AutoVacLauncher main loop (line 578 in autovacuum.c)
  - Self-recursive call for database list rebuilding (line 841)

## Notes and Other Information
- Uses a sophisticated timing algorithm that balances responsiveness with resource efficiency
- Implements safeguards against extreme sleep times (both too short and too long)
- The minimum sleep time prevents busy-waiting and excessive CPU consumption
- The maximum sleep time prevents indefinite delays due to clock issues
- Recursion is limited to one level to avoid infinite loops during database list rebuilding
- When no workers are available, defaults to full autovacuum_naptime sleep duration
- Automatically triggers database list rebuilding when scheduled times are in the past
- Sleep time calculations are based on the next earliest scheduled database maintenance

## Simplified Source

```c
static void launcher_determine_sleep(bool canlaunch, bool recursing, struct timeval *nap)
{
    // If can't launch workers, sleep for full naptime
    if (!canlaunch) {
        nap->tv_sec = autovacuum_naptime;
        nap->tv_usec = 0;
    }
    // Calculate sleep time based on next scheduled database work
    else if (!dlist_is_empty(&DatabaseList)) {
        TimestampTz current_time = GetCurrentTimestamp();
        avl_dbase *next_db = dlist_tail_element(avl_dbase, adl_node, &DatabaseList);

        long secs, usecs;
        TimestampDifference(current_time, next_db->adl_next_worker, &secs, &usecs);

        nap->tv_sec = secs;
        nap->tv_usec = usecs;
    }
    // No databases scheduled, sleep for full naptime
    else {
        nap->tv_sec = autovacuum_naptime;
        nap->tv_usec = 0;
    }

    // If time is in past, rebuild database list (once only)
    if (nap->tv_sec == 0 && nap->tv_usec == 0 && !recursing) {
        rebuild_database_list(InvalidOid);
        launcher_determine_sleep(canlaunch, true, nap);
        return;
    }

    // Enforce minimum sleep time to prevent busy-waiting
    if (nap->tv_sec <= 0 && nap->tv_usec <= MIN_AUTOVAC_SLEEPTIME * 1000) {
        nap->tv_sec = 0;
        nap->tv_usec = MIN_AUTOVAC_SLEEPTIME * 1000;
    }

    // Enforce maximum sleep time to handle clock issues
    if (nap->tv_sec > MAX_AUTOVAC_SLEEPTIME)
        nap->tv_sec = MAX_AUTOVAC_SLEEPTIME;
}
```