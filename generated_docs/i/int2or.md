# int2or

## Location
[src/backend/utils/adt/int.c:1455-1463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1455-L1463)

## Overview
Performs bitwise OR operation between two 16-bit signed integers (smallint type in PostgreSQL).

## Definition
```c
Datum int2or(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2or` function implements the bitwise OR operation for PostgreSQL's `smallint` data type (16-bit signed integers). It takes two `smallint` arguments from the function call context, performs a bitwise OR operation using the C `|` operator, and returns the result as a `smallint` value. This function is typically invoked through PostgreSQL's SQL operator `|` when used with `smallint` operands.

## Parameters / Member Variables
- `arg1`: First 16-bit signed integer operand retrieved via `PG_GETARG_INT16(0)`
- `arg2`: Second 16-bit signed integer operand retrieved via `PG_GETARG_INT16(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT16`: Macro to extract int16 arguments from function call context
  - `PG_RETURN_INT16`: Macro to return int16 result from PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1455-1463`
- Part of PostgreSQL's arithmetic and bitwise operations for integer types
- The function follows PostgreSQL's standard function interface using `PG_FUNCTION_ARGS` and return macros
- Typically accessed through the SQL bitwise OR operator `|` rather than direct function calls