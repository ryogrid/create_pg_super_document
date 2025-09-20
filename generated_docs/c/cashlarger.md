# cashlarger

## Location
[src/backend/utils/adt/cash.c:928-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L928-L942)

## Overview
A PostgreSQL function that returns the larger of two Cash values, implementing the maximum comparison operation for the cash data type.

## Definition

```c
Datum
cashlarger(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL GREATEST/max operation for the Cash data type. It takes two Cash values as arguments and returns whichever value is numerically larger. The function uses a simple ternary conditional operator to perform the comparison and selection. This is typically used to support SQL functions like GREATEST() when applied to cash data types or in contexts where the maximum of two monetary values needs to be determined.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: PostgreSQL's standard function argument structure containing:
  - **c1 (Cash)**: The first cash value for comparison
  - **c2 (Cash)**: The second cash value for comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash arguments from function call
  - PG_RETURN_CASH: Returns the larger Cash value
  - Cash: Cash data type for variables and comparisons
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/cash.c:928-942
- Part of PostgreSQL's cash data type comparison operations
- Uses simple conditional logic (c1 > c2) ? c1 : c2 for maximum determination
- Likely used internally by SQL aggregate functions or GREATEST operations on cash types
- The comparison relies on the underlying numeric representation of Cash values