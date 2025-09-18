# circle_diameter

## Location
[src/backend/utils/adt/geo_ops.c:5043-5053](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5043-L5053)

## Overview
Calculates and returns the diameter of a circle by multiplying the radius by 2.

## Definition
```c
Datum circle_diameter(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_diameter` function computes the diameter of a circle using the standard geometric relationship that diameter equals twice the radius (d = 2r). The function extracts the circle from the PostgreSQL function arguments, multiplies the radius by 2.0 using the float8_mul function, and returns the result as a float8 value. This provides a direct way to obtain the diameter measurement of a circle geometric object.

## Parameters / Member Variables
- `circle`: Input circle for which to calculate the diameter (accessed via PG_GETARG_CIRCLE_P(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P
  - [float8_mul](../f/float8_mul.md)
  - PG_RETURN_FLOAT8
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Implements the simple geometric formula: diameter = 2 × radius
- Uses float8_mul for precise floating-point multiplication with the constant 2.0
- Part of PostgreSQLs geometric data type functions for circle operations
- Returns the result as a PostgreSQL float8 (double precision) value
- The function follows PostgreSQLs V1 calling convention for SQL functions