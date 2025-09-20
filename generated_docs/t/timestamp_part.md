# timestamp_part

## Location
[src/backend/utils/adt/timestamp.c:5611-5616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5611-L5616)

## Overview
A PostgreSQL function that extracts specified date/time fields from timestamp values, serving as a wrapper around the internal  function.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that extracts specific date/time components from timestamp values. It serves as the backend implementation for the  SQL function when operating on timestamp (without time zone) values. The function acts as a simple wrapper that calls the shared implementation  with the  parameter set to , indicating that results should be returned as floating-point numbers rather than numeric types.

## Parameters / Member Variables
- Uses standard PostgreSQL function calling convention ()
- Arguments are accessed through the  structure:
  - Argument 0: Text field name (e.g., 'year', 'month', 'day', 'hour', etc.)
  - Argument 1: Timestamp value to extract from

## Dependencies
- Functions called/Symbols referenced:
  -  (shared implementation for timestamp field extraction)
- Called from (representative examples):
  - SQL function  when used with timestamp arguments
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This function is the non-numeric variant of timestamp field extraction (returns float8)
- Counterpart to  which returns numeric values
- Part of PostgreSQL's date/time function family alongside  for timezone-aware timestamps
- Located in 