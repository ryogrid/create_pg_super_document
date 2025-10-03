# window_lead

## Location
[src/backend/utils/adt/windowfuncs.c:615-626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L615-L626)

## Overview
The window_lead function implements the basic LEAD window function, returning the value of a column from the next row (1 row after) the current row within a partition.

## Definition

```c
Datum
window_lead(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the SQL LEAD window function functionality without offset or default value parameters. It retrieves the value of a specified column from the row that is exactly 1 position after the current row within the same partition. The function is part of PostgreSQL's window function implementation and delegates its core logic to the common leadlag_common function with appropriate parameters to indicate it's a lead operation with no offset and no default value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information structure containing the arguments and context for the window function call
## Dependencies
- Functions called/Symbols referenced:
  - [leadlag_common](../l/leadlag_common.md) (with parameters true, false, false indicating lead operation, no offset, no default)
- Called from:
  - No direct references found (likely called through PostgreSQL's function call mechanism)

## Notes and Other Information
- Located in src/backend/utils/adt/windowfuncs.c:615-626
- This is a wrapper function that delegates to leadlag_common with specific parameters
- The three boolean parameters to leadlag_common represent: is_lead=true (it's a lead), has_offset=false, has_default=false
- Part of PostgreSQL's SQL window function implementation for LEAD(expr)
- This is the simplest version of the LEAD function, defaulting to offset=1 and no default value