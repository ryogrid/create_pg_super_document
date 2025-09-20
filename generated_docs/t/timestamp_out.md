# timestamp_out

## Location
[src/backend/utils/adt/timestamp.c:232-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L232-L257)

## Overview
A PostgreSQL output function that converts internal timestamp values to their external string representation, handling both finite timestamps and special values like infinity.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
This function implements the output conversion for the TIMESTAMP data type (without timezone). It takes PostgreSQL's internal timestamp representation and converts it to a human-readable string format. The function handles both regular timestamp values and special values like 'infinity' and '-infinity'.

The conversion process involves checking for special values first, then breaking down finite timestamps into their component parts (year, month, day, hour, minute, second, microseconds) and formatting them according to the current DateStyle setting. The output format respects PostgreSQL's configuration settings for date and time display.

## Parameters / Member Variables
-  (arg 0): Internal timestamp value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP: Macro to extract timestamp argument
  - TIMESTAMP_NOT_FINITE: Macro to check for infinity values
  - [EncodeSpecialTimestamp](../E/EncodeSpecialTimestamp.md): Handles formatting of special values (infinity, -infinity)
  - [timestamp2tm](timestamp2tm.md): Converts internal timestamp to broken-down time structure
  - [EncodeDateTime](../E/EncodeDateTime.md): Formats broken-down time into string representation
  - [pstrdup](../p/pstrdup.md): Creates a copy of the formatted string for return
  - PG_RETURN_CSTRING: Return value macro
- Called from:
  - [ExecGetJsonValueItemString](../E/ExecGetJsonValueItemString.md) (src/backend/executor/execExprInterp.c:4522)
  - Used as output function for TIMESTAMP type (registered in pg_type catalog)

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE
  - [EncodeSpecialTimestamp](../E/EncodeSpecialTimestamp.md)
  - [timestamp2tm](timestamp2tm.md)
  - [EncodeDateTime](../E/EncodeDateTime.md)
  - DateStyle (global variable)
  - ereport (for error reporting)
- Called from:
  - [ExecGetJsonValueItemString](../E/ExecGetJsonValueItemString.md) (src/backend/executor/execExprInterp.c:4522)

## Notes and Other Information
- Handles special timestamp values: 'infinity' and '-infinity' are formatted as special strings
- Output format depends on the DateStyle setting (ISO, Postgres, SQL, German styles)
- Performs range checking and reports errors for timestamps that cannot be represented
- Returns a newly allocated string that the caller is responsible for freeing
- The function never applies timezone conversion since TIMESTAMP is timezone-naive
- Error handling includes specific error codes for out-of-range values
- Part of PostgreSQL's type system infrastructure for displaying timestamp values