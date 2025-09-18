# timestamptz_trunc

## Location
src/backend/utils/adt/timestamp.c: 4970 - 4987

## Overview
Truncates a timestamp with time zone to a specified time unit using the session timezone for timezone-aware calculations.

## Definition
```c
Datum timestamptz_trunc(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamptz_trunc` function provides timezone-aware timestamp truncation by truncating a TimestampTz value to a specified time unit. This is the main public interface for timezone-aware timestamp truncation operations in PostgreSQL.

The function acts as a thin wrapper around `timestamptz_trunc_internal`, automatically using the current session timezone (`session_timezone`) for the truncation operation. This means that the truncation will respect the user's current timezone setting, handling daylight saving time transitions and other timezone-specific behaviors correctly.

Key characteristics:
- Uses the session's current timezone setting
- Handles infinite timestamps by returning them unchanged
- Delegates the actual truncation logic to `timestamptz_trunc_internal`
- Provides the same time unit support as other truncation functions

## Parameters / Member Variables
- `units` (text*): The time unit to truncate to (e.g., 'year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond', 'week', 'quarter', 'decade', 'century', 'millennium')
- `timestamp` (TimestampTz): The timezone-aware timestamp value to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GETARG_TIMESTAMPTZ
  - TIMESTAMP_NOT_FINITE
  - [timestamptz_trunc_internal](timestamptz_trunc_internal.md)
  - session_timezone (global variable)
  - PG_RETURN_TIMESTAMPTZ
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This is the standard SQL-callable function for timezone-aware timestamp truncation
- Automatically uses the session timezone, making it convenient for typical user operations
- For applications requiring truncation in a specific timezone, `timestamptz_trunc_zone` should be used instead
- Preserves infinite timestamp values without modification
- The actual truncation logic is implemented in `timestamptz_trunc_internal`
- Timezone-aware truncation is essential for correct behavior when dealing with DST transitions
- The function signature follows PostgreSQL's standard function argument pattern