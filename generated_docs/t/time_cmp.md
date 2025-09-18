# time_cmp

## Location
src/backend/utils/adt/date.c: 1734 - 1746

## Overview
Performs a three-way comparison of two time values, returning -1, 0, or 1 based on their relative ordering.

## Definition
```c
Datum time_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `time_cmp` function is a PostgreSQL built-in function that implements a comprehensive comparison operation for the TIME data type. Unlike the binary comparison functions (time_lt, time_le, etc.), this function returns a signed integer indicating the relationship between two time values: -1 if the first time is earlier, 0 if they are equal, and 1 if the first time is later. This three-way comparison function is commonly used by sorting algorithms and indexing operations within PostgreSQL's query execution engine.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `time1` (TimeADT): The first time value to compare (extracted from argument 0)
  - `time2` (TimeADT): The second time value to compare (extracted from argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT: Macro to extract TimeADT values from function arguments
  - PG_RETURN_INT32: Macro to return 32-bit integer result
  - TimeADT: PostgreSQL's internal representation of time values
- Called from (representative examples):
  - [compareDatetime](../c/compareDatetime.md): Used in JSON path execution for datetime comparisons (src/backend/utils/adt/jsonpath_exec.c:3765)

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:1734-1746
- Central comparison function that could be used to implement other time comparison operators
- Uses conditional logic to determine the comparison result rather than a single expression
- Essential for sorting and indexing operations on time columns
- Returns -1 for time1 < time2, 0 for time1 == time2, and 1 for time1 > time2