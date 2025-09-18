# int42eq

## Location
src/backend/utils/adt/int.c: 558 - 566

## Overview
Compares a 32-bit integer with a 16-bit integer for equality, returning a boolean result.

## Definition
```c
Datum int42eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality comparison operator between int4 (32-bit integer) and int2 (16-bit integer) data types in PostgreSQL. It extracts the first argument as a 32-bit integer and the second argument as a 16-bit integer, then performs a direct equality comparison. The 16-bit integer is automatically promoted to 32-bit for the comparison operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call convention macro that provides access to function arguments and context
  - arg1 (int32): The 32-bit integer value extracted from the first function argument
  - arg2 (int16): The 16-bit integer value extracted from the second function argument

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32: Extracts a 32-bit integer from function arguments
  - PG_GETARG_INT16: Extracts a 16-bit integer from function arguments  
  - PG_RETURN_BOOL: Returns a boolean result from the function
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's cross-type comparison operators
- The comparison automatically handles type promotion from int2 to int4
- Used internally by the PostgreSQL query executor when comparing int4 and int2 values
- Location: src/backend/utils/adt/int.c:558-566