# areajoinsel

## Location
[src/backend/utils/adt/geo_selfuncs.c:54-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_selfuncs.c#L54-L66)

## Overview
A join selectivity estimation function for geometric operators that depend on area calculations in join operations.

## Definition
```c
Datum areajoinsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `areajoinsel` function provides join selectivity estimation for geometric operators that involve area calculations when joining two tables. This function is the join counterpart to `areasel`, used by PostgreSQL's query optimizer to estimate how many rows will be produced when joining tables using geometric predicates that depend on area properties.

Like its non-join counterpart, this function returns a hardcoded selectivity value of 0.005 (0.5%). The conservative estimate reflects the same fundamental challenge faced by all geometric selectivity functions: without detailed knowledge of the spatial distribution of geometric data, accurate selectivity estimation is extremely difficult.

This function is specifically designed for join scenarios where geometric area-based operators (such as overlap) are used in join conditions between two tables containing geometric data.

## Parameters / Member Variables
- Uses the standard PostgreSQL function argument macro `PG_FUNCTION_ARGS` which provides access to function call context and arguments, though this specific function doesn't examine any arguments

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_FLOAT8 (PostgreSQL macro for returning float8 values)
- Called from (representative examples):
  - Used by PostgreSQL's query optimizer for join selectivity estimation of geometric area-based operators

## Notes and Other Information
- Returns a hardcoded join selectivity of 0.005 (0.5%)
- Companion function to `areasel` for join scenarios
- Part of the geometric selectivity function family in geo_selfuncs.c
- The conservative estimate encourages the use of geometric indexes in join operations
- Shares the same accuracy limitations as other geometric selectivity functions due to unknown spatial data distribution