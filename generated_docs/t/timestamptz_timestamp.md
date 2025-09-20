# timestamptz_timestamp

## Location
[src/backend/utils/adt/timestamp.c:6365-6372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6365-L6372)

## Overview
This function converts a timestamp with time zone (timestamptz) value to a local timestamp without time zone by applying the session's local timezone conversion.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function serves as a PostgreSQL function wrapper that converts a timestamptz (timestamp with time zone) value to a regular timestamp (without time zone) value. It performs this conversion by interpreting the timestamptz value in the session's current timezone setting and returning the corresponding local timestamp. This function is typically used when you need to strip timezone information from a timestamptz value while preserving the local time representation.

The function is a thin wrapper around the core conversion function , following PostgreSQL's standard pattern for SQL-callable functions that use the PG_FUNCTION_ARGS interface.

## Parameters / Member Variables
- Implicit parameter:  (TimestampTz) - the input timestamp with timezone value retrieved via

## Dependencies
- Functions called/Symbols referenced:
  -  - retrieves the timestamptz argument
  -  - performs the actual timezone conversion
  -  - returns the converted timestamp result
- Called from (representative examples):
  -  (in jsonpath execution)
  -  (timezone conversion function)

## Notes and Other Information
- This function is part of PostgreSQL's datetime/timezone handling system
- The conversion respects the session's current  setting
- The function follows PostgreSQL's V1 calling convention for SQL functions
- Located in  at lines 6365-6372
- The actual conversion logic is delegated to the  helper function