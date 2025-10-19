# int2ne

## Location
[src/backend/utils/adt/int.c:459-467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L459-L467)

## Overview
A PostgreSQL built-in function that compares two 16-bit integers and returns true if they are not equal.

## Definition
Datum int2ne(PG_FUNCTION_ARGS)

## Detailed Description
The int2ne function implements the "!=" or "<>" (not equal) comparison operator for PostgreSQL's int2 (16-bit integer) data type. It takes two int2 values as arguments and returns a boolean result indicating whether the two values are not equal. This function is part of PostgreSQL's type system and is used internally by the SQL engine when processing "!=" or "<>" comparisons between smallint values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - arg1 (int16): The first 16-bit integer value to compare
  - arg2 (int16): The second 16-bit integer value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro to extract int16 arguments)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - SQL engine during query execution for "!=" or "<>" comparisons on smallint columns
  - Expression evaluation subsystem

## Notes and Other Information
- This function is part of the core integer arithmetic operations in PostgreSQL
- It follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- The function is registered in the system catalogs and can be invoked via SQL queries
- Returns true if arg1 != arg2, false otherwise
- Works with smallint (int2) data type which is 16-bit signed integer
- Located in src/backend/utils/adt/int.c:459-467

## Simplified Source

```c
Datum int2ne(PG_FUNCTION_ARGS) {
    // Extract two 16-bit integer arguments
    int16 first_value = PG_GETARG_INT16(0);
    int16 second_value = PG_GETARG_INT16(1);

    // Return true if values are not equal, false otherwise
    PG_RETURN_BOOL(first_value != second_value);
}
```