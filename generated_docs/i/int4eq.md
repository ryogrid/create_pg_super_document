# int4eq

## Location
[src/backend/utils/adt/int.c:396-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L396-L404)

## Overview
Implements the equality comparison operator for 32-bit integers (int4), returning true if both arguments are equal.

## Definition
```c
Datum int4eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality comparison operator (=) for PostgreSQL's int4 (32-bit integer) data type. It takes two int4 values as arguments and returns a boolean result indicating whether they are equal. This function is part of PostgreSQL's comparison operator routines and is used internally when the SQL equality operator is applied to integer values.

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
  - SQL queries using the = operator with int4 values

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:396-404`
- Part of the comparison operator routines section in the int.c file
- This is a standard PostgreSQL V1 calling convention function
- Used internally by PostgreSQL when evaluating expressions like `WHERE column = 5` or `SELECT * FROM table WHERE id = 42`
- Simple implementation using C's native == operator on extracted int32 values
- Returns boolean true if arguments are equal, false otherwise

## Simplified Source

```c
Datum
int4eq(PG_FUNCTION_ARGS)
{
    // Extract the two integer arguments
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    // Return true if equal, false otherwise
    PG_RETURN_BOOL(arg1 == arg2);
}
```