# window_lag

## Location
src/backend/utils/adt/windowfuncs.c: 580 - 591

## Overview
This function implements the basic LAG() window function that returns the value from the previous row in the partition.

## Definition
```c
Datum window_lag(PG_FUNCTION_ARGS)
```

## Detailed Description
The window_lag function implements the SQL LAG() window function in its simplest form - with no offset parameter and no default value. It returns the value of the first argument evaluated on the row that is 1 row before the current row within the same partition.

This is a simple wrapper around the leadlag_common function, passing:
- `forward = false`: Indicates backward direction (LAG vs LEAD)
- `withoffset = false`: No offset parameter, defaults to offset of 1
- `withdefault = false`: No default value parameter

If the previous row doesn't exist (i.e., current row is the first row in the partition), the function returns NULL.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing the value expression to evaluate

## Dependencies
- Functions called/Symbols referenced:
  - [leadlag_common](../l/leadlag_common.md)
- Called from (representative examples):
  - SQL LAG() window function calls through PostgreSQL's function call infrastructure

## Notes and Other Information
- This is the basic form of LAG() with default offset of 1 and no default value
- Returns NULL when trying to access a row before the first row in the partition
- Part of a family of LAG functions including window_lag_with_offset and window_lag_with_offset_and_default
- Follows SQL standard specification for the LAG() window function
- Located in src/backend/utils/adt/windowfuncs.c:580-591