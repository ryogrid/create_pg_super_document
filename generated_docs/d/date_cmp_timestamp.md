# date_cmp_timestamp

## Location
[src/backend/utils/adt/date.c:814-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L814-L822)

## Overview
Provides a three-way comparison between a date value and a timestamp value, returning an integer indicating their relative order.

## Definition

```c
Datum
date_cmp_timestamp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the comparison function used for sorting and ordering operations between DATE and TIMESTAMP types in PostgreSQL. It extracts a DateADT value and a Timestamp value from the function arguments, then delegates the actual comparison logic to the internal helper function date_cmp_timestamp_internal(). The function returns an integer: negative if date < timestamp, zero if date = timestamp, or positive if date > timestamp.

## Parameters / Member Variables
- Function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Argument 0: DateADT value (the date to compare)
- Argument 1: Timestamp value (the timestamp to compare against)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT (macro to extract DateADT from function args)
  - PG_GETARG_TIMESTAMP (macro to extract Timestamp from function args)
  - [date_cmp_timestamp_internal](date_cmp_timestamp_internal.md) (internal comparison function)
  - PG_RETURN_INT32 (macro to return 32-bit integer result)
- Data types used:
  - DateADT (PostgreSQL date type)
  - Timestamp (PostgreSQL timestamp type)
- Called from:
  - Index operations and sorting algorithms
  - ORDER BY clauses involving date and timestamp comparisons

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:814-822
- This function is used internally by PostgreSQL for sorting, indexing, and comparison operations
- Returns -1, 0, or 1 following standard comparison function conventions
- The actual comparison logic is implemented in date_cmp_timestamp_internal() for code reuse
- Used as the basis for all other date/timestamp comparison operators