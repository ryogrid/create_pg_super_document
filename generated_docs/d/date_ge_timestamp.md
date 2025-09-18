# date_ge_timestamp

## Location
[src/backend/utils/adt/date.c:805-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L805-L813)

## Overview
Compares a date value with a timestamp value to determine if the date is greater than or equal to the timestamp.

## Definition


## Detailed Description
This function implements the greater-than-or-equal-to comparison operator (>=) between a DATE type and a TIMESTAMP type in PostgreSQL. It extracts a DateADT value and a Timestamp value from the function arguments, then delegates the actual comparison logic to the internal helper function date_cmp_timestamp_internal(). The function returns true if the date value is greater than or equal to the timestamp value, false otherwise.

## Parameters / Member Variables
- Function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Argument 0: DateADT value (the date to compare)
- Argument 1: Timestamp value (the timestamp to compare against)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT (macro to extract DateADT from function args)
  - PG_GETARG_TIMESTAMP (macro to extract Timestamp from function args)
  - [date_cmp_timestamp_internal](date_cmp_timestamp_internal.md) (internal comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Data types used:
  - DateADT (PostgreSQL date type)
  - Timestamp (PostgreSQL timestamp type)
- Called from:
  - SQL operator functions (as part of date >= timestamp comparisons)

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:805-813
- This function is typically called through PostgreSQL's function manager when the >= operator is used between date and timestamp values in SQL
- The actual comparison logic is implemented in date_cmp_timestamp_internal() for code reuse across different comparison operators
- Returns a PostgreSQL Datum containing a boolean value