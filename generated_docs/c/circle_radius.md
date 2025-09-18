# circle_radius

## Location
src/backend/utils/adt/geo_ops.c: 5054 - 5065

## Overview
Returns the radius value of a circle geometric object directly without any calculation.

## Definition
```c
Datum circle_radius(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_radius` function is a simple accessor function that extracts and returns the radius field from a circle geometric object. Unlike other circle measurement functions that perform calculations, this function directly returns the stored radius value from the circle structure. It serves as a SQL-callable interface to access the fundamental radius property of a circle.

## Parameters / Member Variables
- `circle`: Input circle from which to extract the radius (accessed via PG_GETARG_CIRCLE_P(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P
  - PG_RETURN_FLOAT8
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is the simplest circle measurement function, directly returning the stored radius value
- No mathematical calculations are performed - its a direct field access
- Part of PostgreSQLs geometric data type functions for circle operations
- Returns the result as a PostgreSQL float8 (double precision) value
- The function follows PostgreSQLs V1 calling convention for SQL functions
- The radius is a fundamental property of the CIRCLE structure in PostgreSQLs geometric types