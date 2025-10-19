# float8um

## Location
[src/backend/utils/adt/float.c:662-671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L662-L671)

## Overview
Implements the unary minus operation for double-precision floating-point numbers in PostgreSQL's SQL function interface.

## Definition
```c
Datum float8um(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the unary minus (negation) operation for PostgreSQL's float8 (double-precision floating-point) data type. It extracts a float8 value from the function arguments, applies the unary minus operator to negate the value, and returns the negated result as a PostgreSQL Datum. The function is part of PostgreSQL's base operations for float8 arithmetic and handles the SQL unary minus operator for double-precision numbers.

## Parameters / Member Variables
- `arg1`: The input float8 value obtained via `PG_GETARG_FLOAT8(0)` from the first function argument
- `result`: Local variable storing the negated value before return

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8` (macro for extracting float8 argument)
  - `PG_RETURN_FLOAT8` (macro for returning float8 result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:662-671
- Part of PostgreSQL's float8 base operations suite
- Implements unary minus using simple C negation operator
- Follows PostgreSQL's standard function call convention with `PG_FUNCTION_ARGS`
- Returns result using PostgreSQL's Datum system

## Simplified Source

```c
Datum
float8um(PG_FUNCTION_ARGS)
{
    // Extract the float8 argument
    float8 arg1 = PG_GETARG_FLOAT8(0);

    // Apply unary minus operation and return result
    PG_RETURN_FLOAT8(-arg1);
}
```