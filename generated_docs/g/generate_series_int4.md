# generate_series_int4

## Location
[src/backend/utils/adt/int.c:1503-1508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1503-L1508)

## Overview
A wrapper function that generates a series of 32-bit integers, delegating to the step-based implementation with default step value.

## Definition
```c
Datum generate_series_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
The `generate_series_int4` function serves as a non-persistent numeric series generator for 32-bit integers (int4/integer type). It acts as a simplified entry point that internally calls `generate_series_step_int4` to handle the actual series generation logic. This function is typically used when generating a sequence of integers with an implicit step of 1, providing a more convenient interface for the common case where no explicit step value is needed. The function follows PostgreSQL's set-returning function (SRF) pattern to return multiple rows.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the input arguments (start and end values for the series)

## Dependencies
- Functions called/Symbols referenced:
  - [generate_series_step_int4](generate_series_step_int4.md) - The actual implementation that handles series generation with step support
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1503-1508`
- This is a wrapper function that simplifies the interface for the two-parameter version of generate_series()
- The function comment indicates it's a "non-persistent numeric series generator"
- Part of PostgreSQL's generate_series() family of functions for creating sequential data
- Returns a `Datum` type following PostgreSQL's function calling convention
- The actual series generation logic is implemented in `generate_series_step_int4`

## Simplified Source
```c
/*
 * Non-persistent numeric series generator - wrapper function
 */
Datum generate_series_int4(PG_FUNCTION_ARGS) {
    // Delegate to the step-based implementation with default step of 1
    return generate_series_step_int4(fcinfo);
}
```