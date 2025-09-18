# date_lt_timestamptz

## Location
[src/backend/utils/adt/date.c:862-870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L862-L870)

## Overview
Compares a DATE value with a TIMESTAMPTZ value for less-than ordering, returning true if the date is earlier than the timestamptz.

## Definition
```c
Datum date_lt_timestamptz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than comparison operator between a DATE and a TIMESTAMPTZ value. It extracts the date arguments using PostgreSQL's function argument macros and delegates the actual comparison logic to the internal helper function `date_cmp_timestamptz_internal`. The function returns true (as a PostgreSQL boolean Datum) if the comparison result is less than 0, indicating the DATE value represents an earlier date than the TIMESTAMPTZ value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `DateADT` - The date value to compare
  - Argument 1: `TimestampTz` - The timestamptz value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT`: Extracts DATE argument from function call
  - `PG_GETARG_TIMESTAMPTZ`: Extracts TIMESTAMPTZ argument from function call
  - [date_cmp_timestamptz_internal](date_cmp_timestamptz_internal.md): Performs the actual date comparison logic
  - `PG_RETURN_BOOL`: Returns boolean result as PostgreSQL Datum
- Called from: 
  - This function is typically invoked through PostgreSQL's operator system for the '<' operator between DATE and TIMESTAMPTZ types

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:862-870
- This is a SQL-callable function that implements the '<' operator for DATE < TIMESTAMPTZ comparisons
- The function handles timezone considerations through the internal comparison function
- Part of PostgreSQL's type system for cross-type date/timestamp comparisons and ordering operations