# date_timestamptz

## Location
[src/backend/utils/adt/date.c:1327-1341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1327-L1341)

## Overview
Converts a PostgreSQL date value to a timestamp with time zone (timestamptz) data type, adding time component as 00:00:00 in the current timezone.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a PostgreSQL built-in function that converts a date value (DateADT) to a timestamp with time zone value (TimestampTz). This conversion effectively adds a time component of 00:00:00 to the date in the current timezone setting, creating a timestamptz that represents the beginning of that date in the local timezone. The function is implemented as a PostgreSQL V1 calling convention function, taking arguments through the  macro and returning a .

The conversion is performed by calling the internal helper function , which handles the actual conversion logic, timezone considerations, and potential overflow checking.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Argument 0:  - The input date value to be converted

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract date argument
  -  - Internal helper function for conversion with timezone
  -  - Macro to return timestamp result
- Types used:
  -  - PostgreSQL date type
  -  - PostgreSQL timestamp with timezone type
  -  - PostgreSQL generic return type
- Called from (representative examples):
  -  (in jsonpath execution)

## Notes and Other Information
- This function is part of PostgreSQL's date/time type conversion system
- The conversion sets the time component to 00:00:00 (start of day) in the current timezone
- Unlike , this function produces a timezone-aware result
- Overflow checking is performed by the underlying  function
- The timezone used depends on the current PostgreSQL timezone setting
- Located in
- Used primarily in SQL contexts where implicit or explicit conversion from date to timestamptz is needed
- The resulting timestamptz represents the start of the specified date in the current timezone

## Simplified Source

```c
Datum date_timestamptz(PG_FUNCTION_ARGS) {
    DateADT dateVal = PG_GETARG_DATEADT(0);
    TimestampTz result;

    // Convert date to timestamptz (adds 00:00:00 time in session timezone)
    result = date2timestamptz(dateVal);

    PG_RETURN_TIMESTAMP(result);
}
```