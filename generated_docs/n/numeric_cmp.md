# numeric_cmp

## Location
src/backend/utils/adt/numeric.c: 2415 - 2430

## Overview
PostgreSQL built-in function that performs three-way comparison between two numeric values, returning an integer indicating their relative ordering.

## Definition
```c
Datum numeric_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_cmp` function is a PostgreSQL built-in function that implements three-way comparison for the numeric data type. It takes two numeric arguments and returns an integer value indicating their relative ordering: negative if the first argument is less than the second, zero if they are equal, and positive if the first argument is greater than the second.

This function serves as the standard comparison function for numeric values and is used extensively throughout PostgreSQL for ordering operations, equality checks, and various comparison-based algorithms. It delegates the actual comparison logic to the internal `cmp_numerics` function while handling the PostgreSQL function call interface and memory management.

The function follows PostgreSQL's standard function calling conventions, using the PG_FUNCTION_ARGS macro to access arguments and PG_RETURN_INT32 to return the result.

## Parameters / Member Variables
- Argument 0: First numeric value to compare (accessed via PG_GETARG_NUMERIC(0))
- Argument 1: Second numeric value to compare (accessed via PG_GETARG_NUMERIC(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (extracts Numeric arguments from function call)
  - cmp_numerics (performs the actual numeric comparison)
  - PG_FREE_IF_COPY (frees copied numeric values if necessary)
  - PG_RETURN_INT32 (returns integer result)
  - Numeric (PostgreSQL numeric data type)
- Called from (representative examples):
  - compareJsonbScalarValue (JSON-B scalar value comparison)
  - compareNumeric (JSON path execution numeric comparison)

## Notes and Other Information
- This is an ordinary comparison function, distinct from the optimized sort support comparisons
- Returns standard three-way comparison result: <0, 0, or >0
- Properly handles PostgreSQL's memory management with PG_FREE_IF_COPY calls
- Part of PostgreSQL's standard operator framework for the numeric data type
- Can be called directly from SQL as well as from internal C code
- The function signature follows PostgreSQL's V1 calling convention