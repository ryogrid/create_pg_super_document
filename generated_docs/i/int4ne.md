# int4ne

## Location
src/backend/utils/adt/int.c: 405 - 413

## Overview
Implements the inequality comparison operator for 32-bit integers (int4), returning true if the arguments are not equal.

## Definition
```c
Datum int4ne(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inequality comparison operator (!=, <>) for PostgreSQL's int4 (32-bit integer) data type. It takes two int4 values as arguments and returns a boolean result indicating whether they are not equal. This function is part of PostgreSQL's comparison operator routines and is used internally when the SQL inequality operator is applied to integer values.

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
  - SQL queries using the != or <> operator with int4 values

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:405-413`
- Part of the comparison operator routines section in the int.c file
- This is a standard PostgreSQL V1 calling convention function
- Used internally by PostgreSQL when evaluating expressions like `WHERE column != 5` or `SELECT * FROM table WHERE id <> 42`
- Simple implementation using C's native != operator on extracted int32 values
- Returns boolean true if arguments are not equal, false if they are equal
- Complement function to int4eq