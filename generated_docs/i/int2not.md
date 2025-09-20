# int2not

## Location
[src/backend/utils/adt/int.c:1473-1481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1473-L1481)

## Overview
Performs bitwise NOT (complement) operation on a 16-bit signed integer (smallint type in PostgreSQL).

## Definition
```c
Datum int2not(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2not` function implements the bitwise NOT operation for PostgreSQL's `smallint` data type (16-bit signed integers). It takes one `smallint` argument from the function call context, performs a bitwise complement operation using the C `~` operator (flipping all bits), and returns the result as a `smallint` value. This function is typically invoked through PostgreSQL's SQL operator `~` when used with `smallint` operands.

## Parameters / Member Variables
- `arg1`: The 16-bit signed integer operand to be complemented, retrieved via `PG_GETARG_INT16(0)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT16`: Macro to extract int16 argument from function call context
  - `PG_RETURN_INT16`: Macro to return int16 result from PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1473-1481`
- Part of PostgreSQL's arithmetic and bitwise operations for integer types
- Unlike the other bitwise operations (AND, OR, XOR), this is a unary operation taking only one operand
- The function follows PostgreSQL's standard function interface using `PG_FUNCTION_ARGS` and return macros
- Typically accessed through the SQL bitwise NOT operator `~` rather than direct function calls