# launcher_determine_sleep

## Location
src/backend/postmaster/autovacuum.c: 792 - 875

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