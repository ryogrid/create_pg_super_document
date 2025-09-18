# leapadd

## Location
src/timezone/zic.c: 3403 - 3424

## Overview
The leapadd function inserts a new leap second entry into the global leap second tables, maintaining chronological order and tracking leap second corrections for accurate timekeeping.

## Definition


## Detailed Description
The leapadd function manages leap second data within PostgreSQL's timezone compiler by inserting new leap second entries while preserving chronological order. It validates that the maximum number of leap seconds hasn't been exceeded, finds the correct insertion point based on the timestamp, and shifts existing entries to accommodate the new leap second. The function maintains three parallel arrays tracking transition times, correction values, and rolling indicators, ensuring data consistency across all leap second information.

## Parameters / Member Variables
- : A zic_t timestamp indicating when the leap second occurs
- : An integer representing the cumulative leap second correction value at this time
- : An integer flag indicating whether this leap second uses rolling time semantics

## Dependencies
- Functions called/Symbols referenced:
  - TZ_MAX_LEAPS (maximum leap second limit constant)
  - EXIT_FAILURE (error exit status)
  - zic_t (timestamp type definition)
- Called from (representative examples):
  - inleap (at line 1780)

## Notes and Other Information
- Maintains chronological order by finding the correct insertion point and shifting subsequent entries
- Uses memmove() for safe array element shifting to avoid data corruption
- Exits with failure if TZ_MAX_LEAPS limit is exceeded
- Updates three global arrays: trans[] (timestamps), corr[] (corrections), roll[] (rolling flags)
- Increments the global leapcnt counter to track total leap second entries
- Essential for accurate time calculations that must account for leap second adjustments