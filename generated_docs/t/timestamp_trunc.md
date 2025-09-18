# timestamp_trunc

## Location
src/backend/utils/adt/timestamp.c: 4618 - 4751

## Overview
Truncates a timestamp to a specified time unit, effectively rounding down to the beginning of the specified time period.

## Definition
```c
Datum timestamp_trunc(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamp_trunc` function truncates a timestamp value to a specified time unit (such as 'year', 'month', 'day', 'hour', etc.). This is commonly used for time-series analysis to group timestamps into regular intervals. The function works by:

1. Converting the timestamp to a broken-down time structure
2. Based on the specified unit, zeroing out all smaller time components
3. For larger units (decade, century, millennium), calculating the appropriate boundary
4. Converting the modified time structure back to a timestamp

The function supports a wide range of time units from microseconds up to millennia, with special handling for weeks (using ISO week calculations), quarters, and larger calendar periods.

## Parameters / Member Variables
- `units` (text*): The time unit to truncate to (e.g., 'year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond', 'week', 'quarter', 'decade', 'century', 'millennium')
- `timestamp` (Timestamp): The timestamp value to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GETARG_TIMESTAMP
  - TIMESTAMP_NOT_FINITE
  - downcase_truncate_identifier
  - DecodeUnits
  - timestamp2tm
  - tm2timestamp
  - date2isoweek
  - isoweek2date
  - format_type_be
  - PG_RETURN_TIMESTAMP
  - Various DTK_* constants (DTK_WEEK, DTK_YEAR, DTK_MONTH, etc.)
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Supports truncation from microseconds to millennia with proper handling of calendar boundaries
- Week truncation uses ISO week calculations, which may result in dates from the previous or next year
- For negative years, millennium/century/decade calculations use special logic to handle BC dates correctly  
- Quarter truncation rounds down to the beginning of the quarter (Jan, Apr, Jul, Oct)
- The function preserves infinite timestamp values without modification
- Includes comprehensive error handling for invalid units and out-of-range values
- Uses a case-insensitive unit string matching system