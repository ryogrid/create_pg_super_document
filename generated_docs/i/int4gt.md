# int4gt

## Location
[src/backend/utils/adt/int.c:432-440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L432-L440)

## Overview
A PostgreSQL built-in function that compares two 32-bit integers and returns true if the first integer is greater than the second.

## Definition
Datum int4gt(PG_FUNCTION_ARGS)

## Detailed Description
The int4gt function implements the ">" (greater than) comparison operator for PostgreSQL's int4 (32-bit integer) data type. It takes two int4 values as arguments and returns a boolean result indicating whether the first value is greater than the second value. This function is part of PostgreSQL's type system and is used internally by the SQL engine when processing ">" comparisons between integer values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - arg1 (int32): The first integer value to compare
  - arg2 (int32): The second integer value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro to extract int32 arguments)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - SQL engine during query execution for ">" comparisons
  - Expression evaluation subsystem

## Notes and Other Information
- This function is part of the core integer arithmetic operations in PostgreSQL
- It follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- The function is registered in the system catalogs and can be invoked via SQL queries
- Returns true if arg1 > arg2, false otherwise
- Located in src/backend/utils/adt/int.c:432-440

## Simplified Source

```c
Datum int4gt(PG_FUNCTION_ARGS) {
    // Extract two 32-bit integer arguments
    int32 first_value = PG_GETARG_INT32(0);
    int32 second_value = PG_GETARG_INT32(1);

    // Return true if first > second, false otherwise
    PG_RETURN_BOOL(first_value > second_value);
}
```