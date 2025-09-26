# pg_next_dst_boundary

## Location
src/timezone/localtime.c: 1610 - 1756

## Overview
Finds the next daylight saving time (DST) transition boundary after a given timestamp within a specified timezone.

## Definition


## Detailed Description
The `pg_next_dst_boundary` function locates the next DST transition point after a specified timestamp within a given timezone. It returns detailed information about the timezone state both before and after the transition, including GMT offsets and DST status.

The function handles several scenarios:
1. **DST-less zones**: Returns 0 with current timezone information
2. **Extrapolation**: For timestamps outside the transition table, it extrapolates using repeating patterns
3. **Binary search**: For timestamps within the transition table, it uses binary search to efficiently locate the next boundary
4. **Edge cases**: Handles times before the first transition or after the last known transition

The function is critical for PostgreSQL's timezone handling, particularly for operations that need to account for DST transitions when performing date/time arithmetic.

## Parameters / Member Variables
- `timep`: Pointer to the timestamp to find the next DST boundary after
- `before_gmtoff`: Output parameter for GMT offset before the boundary
- `before_isdst`: Output parameter for DST status before the boundary
- `boundary`: Output parameter for the timestamp of the DST boundary
- `after_gmtoff`: Output parameter for GMT offset after the boundary
- `after_isdst`: Output parameter for DST status after the boundary
- `tz`: Timezone structure containing transition information

## Dependencies
- Functions called/Symbols referenced:
  - pg_next_dst_boundary (recursive call for extrapolation)
  - pg_time_t, pg_tz, ttinfo (timezone-related types)
  - YEARSPERREPEAT, AVGSECSPERYEAR (constants for extrapolation)
- Called from (representative examples):
  - DetermineTimeZoneOffsetInternal (for timezone offset calculations)

## Notes and Other Information
- Returns 1 if a DST boundary is found, 0 if no boundary exists after the given time, -1 on failure
- Uses binary search for efficient lookup within transition tables
- Supports extrapolation for timestamps outside known transition data using repeating patterns
- Handles both forward and backward extrapolation for historical and future dates
- Essential for accurate timezone calculations in PostgreSQL's datetime functionality
- The function is part of PostgreSQL's public timezone API