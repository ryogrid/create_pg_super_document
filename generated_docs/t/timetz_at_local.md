# timetz_at_local

## Location
[src/backend/utils/adt/date.c:3165-3172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L3165-L3172)

## Overview
Converts a time with time zone to the session's local timezone, providing a convenient function for displaying times in the user's local timezone context.

## Definition

```c
Datum
timetz_at_local(PG_FUNCTION_ARGS)
```
## Detailed Description
`timetz_at_local` is a PostgreSQL built-in function that converts a time with time zone (TIMETZ) value to the local timezone as defined by the current session's `timezone` parameter. This function serves as a convenience wrapper that automatically determines the session timezone and delegates the actual conversion to `timetz_zone`. Unlike the equivalent functions for timestamp types, this function maintains the TIMETZ type (it doesn't flip between time with and without timezone) since TIME type doesn't carry timezone information that would make sense without an explicit timezone context.

The function retrieves the session timezone name and passes it to `timetz_zone` for the actual conversion logic, ensuring consistent behavior across the timezone conversion family of functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `time` (Datum): The input time with time zone value to convert to local time

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM
  - [pg_get_timezone_name](../p/pg_get_timezone_name.md)
  - session_timezone
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - cstring_to_text
  - DirectFunctionCall2
  - [timetz_zone](timetz_zone.md)
- Called from (representative examples):
  - SQL AT LOCAL expressions with TIMETZ values
  - Applications requiring local timezone display of time values

## Notes and Other Information
- Uses `session_timezone` global variable to determine the target local timezone
- Maintains TIMETZ type throughout conversion (no type switching like timestamp functions)
- Acts as a convenience wrapper around `timetz_zone` with automatic timezone detection
- The session timezone is resolved at function call time, making results dependent on session settings
- Particularly useful for applications that need to display times in the user's preferred timezone
- More efficient than manually querying the session timezone and calling `timetz_zone`
- Inherits all timezone conversion logic and DST handling from the underlying `timetz_zone` function