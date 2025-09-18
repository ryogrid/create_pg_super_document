# time2t

## Location
src/backend/utils/adt/timestamp.c: 2128 - 2133

## Overview
A static helper function that converts individual time components (hour, minute, second, fractional second) into a single TimeOffset value representing microseconds since midnight.

## Definition


## Detailed Description
The time2t function performs a time-to-offset conversion by calculating the total number of microseconds represented by the given time components. It multiplies hours by minutes per hour, adds minutes, multiplies by seconds per minute, adds seconds, multiplies by microseconds per second, and finally adds the fractional seconds (already in microseconds). This creates a linear time offset suitable for timestamp calculations.

## Parameters / Member Variables
- `hour`: The hour component (0-23)
- `min`: The minute component (0-59) 
- `sec`: The second component (0-59)
- `fsec`: The fractional second component in microseconds (fsec_t type)

## Dependencies
- Functions called/Symbols referenced:
  - fsec_t (fractional seconds type)
  - MINS_PER_HOUR (constant for minutes per hour)
  - SECS_PER_MINUTE (constant for seconds per minute) 
  - USECS_PER_SEC (constant for microseconds per second)
  - Timestamp (return type context)
- Called from (representative examples):
  - tm2timestamp (in timestamp.c and ecpg pgtypeslib)

## Notes and Other Information
This is a static function local to timestamp.c, used internally for timestamp conversion operations. The function performs straightforward arithmetic to convert time components into a microsecond offset, which is the internal representation used by PostgreSQL's timestamp system.