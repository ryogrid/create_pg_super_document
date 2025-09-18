# window_lag_with_offset

## Location
[src/backend/utils/adt/windowfuncs.c:592-602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L592-L602)

## Overview
The window_lag_with_offset function implements the LAG window function with an offset parameter, returning the value of a column from a row that is a specified number of positions before the current row within a partition.

## Definition


## Detailed Description
This function provides the SQL LAG window function functionality with offset capability. It retrieves the value of a specified column from a row that is exactly 'offset' rows before the current row within the same partition. The function is part of PostgreSQL's window function implementation and delegates its core logic to the common leadlag_common function with appropriate parameters to indicate it's a lag operation with offset but without a default value.

## Parameters / Member Variables
- : Function call information structure containing the arguments and context for the window function call

## Dependencies
- Functions called/Symbols referenced:
  - [leadlag_common](../l/leadlag_common.md) (with parameters false, true, false indicating lag operation, with offset, no default)
- Called from:
  - No direct references found (likely called through PostgreSQL's function call mechanism)

## Notes and Other Information
- Located in src/backend/utils/adt/windowfuncs.c:592-602
- This is a wrapper function that delegates to leadlag_common with specific parameters
- The three boolean parameters to leadlag_common represent: is_lead=false (it's a lag), has_offset=true, has_default=false
- Part of PostgreSQL's SQL window function implementation for LAG(expr, offset)