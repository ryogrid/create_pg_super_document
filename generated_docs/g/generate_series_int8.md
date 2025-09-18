# generate_series_int8

## Location
src/backend/utils/adt/int8.c: 1377 - 1382

## Overview
A wrapper function that generates a non-persistent numeric series of 64-bit integers by delegating to the step-based series generator.

## Definition
```c
Datum generate_series_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a simplified entry point for generating a series of int8 (bigint) values. It is designed as a non-persistent numeric series generator that delegates all its functionality to the more comprehensive `generate_series_step_int8` function. This pattern allows PostgreSQL to provide both a two-parameter version (start, stop) and a three-parameter version (start, stop, step) of the generate_series function for int8 data types.

The function essentially acts as a wrapper that forwards the function call information directly to the step-based implementation, which handles the actual series generation logic.

## Parameters / Member Variables
- Input: Function call information passed through PG_FUNCTION_ARGS (typically start and stop values for the series)
- Return: A Datum containing the generated series result

## Dependencies
- Functions called/Symbols referenced:
  - generate_series_step_int8 (the main implementation function)

- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This function implements the two-parameter version of generate_series for int8 values
- The actual series generation logic is handled by generate_series_step_int8
- Part of PostgreSQL's built-in series generation functions for numeric types
- Located in src/backend/utils/adt/int8.c:1377-1382
- Marked as non-persistent, meaning it generates values on-demand rather than storing them