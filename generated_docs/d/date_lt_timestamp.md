# date_lt_timestamp

## Location
src/backend/utils/adt/date.c: 778 - 786

## Overview
PostgreSQL built-in function that tests whether a date value is less than a timestamp value.

## Definition
```c
Datum date_lt_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than operator (<) for comparing date and timestamp data types in PostgreSQL. It serves as a SQL-callable function that enables crosstype comparisons in queries. The function extracts the date and timestamp arguments from the PostgreSQL function call interface and delegates the actual comparison logic to the internal comparison function.

The function follows PostgreSQL's function call convention using the PG_FUNCTION_ARGS interface and returns a PostgreSQL boolean datum indicating whether the date (interpreted as midnight of that day) represents an earlier point in time than the given timestamp.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: DateADT value (extracted via PG_GETARG_DATEADT(0))
  - Argument 1: Timestamp value (extracted via PG_GETARG_TIMESTAMP(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT (macro to extract date argument from function call)
  - PG_GETARG_TIMESTAMP (macro to extract timestamp argument from function call)
  - date_cmp_timestamp_internal (internal comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
  - DateADT (PostgreSQL's date type)
  - Timestamp (PostgreSQL's timestamp type)
- Called from (representative examples):
  - Used as SQL operator function (registered in PostgreSQL's operator catalog)
  - Called when SQL expressions like `date_column < timestamp_column` are evaluated

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL queries
- Returns true if the date (interpreted as midnight) is earlier than the timestamp
- Part of PostgreSQL's crosstype operator system enabling mixed temporal type comparisons
- The actual comparison logic is delegated to date_cmp_timestamp_internal for consistency
- Part of a family of comparison operators (=, !=, <, >, <=, >=) between dates and timestamps
- Located in src/backend/utils/adt/date.c:778-786
- Uses PostgreSQL's standard function interface (Datum return type, PG_FUNCTION_ARGS)
- The function is likely registered as an operator function in pg_operator catalog
- Useful for range queries and temporal filtering in SQL queries involving mixed date/timestamp columns