# int4lt

## Location
[src/backend/utils/adt/int.c:414-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L414-L422)

## Overview
Implements the less-than comparison operator for 32-bit integers (int4), returning true if the first argument is less than the second.

## Definition
```c
Datum int4lt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than comparison operator (<) for PostgreSQL's int4 (32-bit integer) data type. It takes two int4 values as arguments and returns a boolean result indicating whether the first argument is less than the second argument. This function is part of PostgreSQL's comparison operator routines and is used internally when the SQL less-than operator is applied to integer values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments through the function call context
  - Argument 0: First int4 value for comparison (accessed via `PG_GETARG_INT32(0)`)
  - Argument 1: Second int4 value for comparison (accessed via `PG_GETARG_INT32(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Macro to extract int32 arguments from function arguments
  - `PG_RETURN_BOOL`: Macro to return boolean result from PostgreSQL function
- Called from (representative examples):
  - PostgreSQL operator evaluation system (no direct references found in current analysis)
  - SQL queries using the < operator with int4 values

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:414-422`
- Part of the comparison operator routines section in the int.c file
- This is a standard PostgreSQL V1 calling convention function
- Used internally by PostgreSQL when evaluating expressions like `WHERE column < 5` or `SELECT * FROM table WHERE id < 42`
- Simple implementation using C's native < operator on extracted int32 values
- Returns boolean true if first argument is less than second, false otherwise
- Forms part of the complete set of integer comparison operators (eq, ne, lt, le, gt, ge)