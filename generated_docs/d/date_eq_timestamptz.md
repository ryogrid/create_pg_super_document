# date_eq_timestamptz

## Location
[src/backend/utils/adt/date.c:844-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L844-L852)

## Overview
Compares a DATE value with a TIMESTAMPTZ value for equality, returning true if they represent the same date.

## Definition

```c
Datum
date_eq_timestamptz(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the equality comparison operator between a DATE and a TIMESTAMPTZ value. It extracts the date arguments using PostgreSQL's function argument macros and delegates the actual comparison logic to the internal helper function . The function returns true (as a PostgreSQL boolean Datum) if the comparison result equals 0, indicating the dates are equal.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The date value to compare
  - Argument 1:  - The timestamptz value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts DATE argument from function call
  - : Extracts TIMESTAMPTZ argument from function call
  - : Performs the actual date comparison logic
  - : Returns boolean result as PostgreSQL Datum
- Called from: 
  - This function is typically invoked through PostgreSQL's operator system for the '=' operator between DATE and TIMESTAMPTZ types

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:844-852
- This is a SQL-callable function that implements the '=' operator for DATE = TIMESTAMPTZ comparisons
- The function handles timezone considerations through the internal comparison function
- Part of PostgreSQL's type system for cross-type date/timestamp comparisons

## Simplified Source

```c
Datum
date_eq_timestamptz(PG_FUNCTION_ARGS)
{
    // Extract date and timestamptz arguments
    DateADT dateVal = PG_GETARG_DATEADT(0);
    TimestampTz timestamptz = PG_GETARG_TIMESTAMPTZ(1);

    // Return true if date equals timestamptz (comparison result == 0)
    PG_RETURN_BOOL(date_cmp_timestamptz_internal(dateVal, timestamptz) == 0);
}
```