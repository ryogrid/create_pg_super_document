# date_ne_timestamp

## Location
[src/backend/utils/adt/date.c:769-777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L769-L777)

## Overview
PostgreSQL built-in function that tests inequality between a date value and a timestamp value.

## Definition
```c
Datum date_ne_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inequality operator (!=, <>) for comparing date and timestamp data types in PostgreSQL. It serves as a SQL-callable function that enables crosstype comparisons in queries. The function extracts the date and timestamp arguments from the PostgreSQL function call interface and delegates the actual comparison logic to the internal comparison function.

The function follows PostgreSQL's function call convention using the PG_FUNCTION_ARGS interface and returns a PostgreSQL boolean datum indicating whether the date and timestamp represent different points in time (when the date is interpreted as midnight of that day).

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: DateADT value (extracted via PG_GETARG_DATEADT(0))
  - Argument 1: Timestamp value (extracted via PG_GETARG_TIMESTAMP(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT (macro to extract date argument from function call)
  - PG_GETARG_TIMESTAMP (macro to extract timestamp argument from function call)
  - [date_cmp_timestamp_internal](date_cmp_timestamp_internal.md) (internal comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
  - DateADT (PostgreSQL's date type)
  - Timestamp (PostgreSQL's timestamp type)
- Called from (representative examples):
  - Used as SQL operator function (registered in PostgreSQL's operator catalog)
  - Called when SQL expressions like `date_column != timestamp_column` or `date_column <> timestamp_column` are evaluated

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL queries
- Returns true if the date (interpreted as midnight) does not equal the timestamp
- Part of PostgreSQL's crosstype operator system enabling mixed temporal type comparisons
- The actual comparison logic is delegated to date_cmp_timestamp_internal for consistency
- Complementary function to date_eq_timestamp (returns opposite boolean result)
- Located in src/backend/utils/adt/date.c:769-777
- Uses PostgreSQL's standard function interface (Datum return type, PG_FUNCTION_ARGS)
- The function is likely registered as an operator function in pg_operator catalog for both != and <> operators