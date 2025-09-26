# generate_series_numeric

## Location
[src/backend/utils/adt/numeric.c:1701-1706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1701-L1706)

## Overview
A wrapper function that generates a series of numeric values by delegating to the step-based generate_series_step_numeric function.

## Definition

```c
Datum
generate_series_numeric(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a simple wrapper that implements the two-parameter version of PostgreSQL's generate_series function for numeric data types. It takes a start and end value and generates a series with an implicit step of 1. The function immediately delegates all work to , passing through the function call information () which contains the function arguments and context.

This design pattern allows for a unified implementation where the two-parameter version is simply a special case of the three-parameter version with a default step value.

## Parameters / Member Variables
- Input parameters (accessed via fcinfo): Start and end values for the numeric series (step defaults to 1)

## Dependencies
- Functions called/Symbols referenced:
  - generate_series_step_numeric - The actual implementation that handles series generation with explicit step parameter
- Called from:
  - No direct references found (typically called via SQL function calls)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:1701-1706
- Acts as a convenience wrapper for the more general step-based series generation
- Part of PostgreSQL's set-returning function family for numeric types
- The actual series generation logic is implemented in 
- Follows PostgreSQL's pattern of providing both two-parameter and three-parameter versions of generate_series