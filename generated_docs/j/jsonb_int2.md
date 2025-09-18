# jsonb_int2

## Location
src/backend/utils/adt/jsonb.c: 2073 - 2090

## Overview
Converts a JSONB numeric value to a PostgreSQL smallint (int2) type, using numeric-to-smallint conversion with range checking.

## Definition
```c
Datum jsonb_int2(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_int2` function extracts a numeric value from a JSONB input and converts it to PostgreSQL's `smallint` (int2) type. It first validates that the JSONB contains a scalar numeric value, then delegates the actual conversion to the `numeric_int2` function via `DirectFunctionCall1`. This approach ensures that proper range checking and conversion logic is applied, as `numeric_int2` handles bounds checking for the smallint type (-32,768 to 32,767).

The function follows a two-step conversion process: first extracting the numeric value from JSONB, then converting that numeric to smallint using PostgreSQL's existing numeric conversion infrastructure.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `in`: The input JSONB value to be converted to smallint
- `retValue`: Local variable holding the converted smallint value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Extracts JSONB argument from function arguments
  - `JsonbExtractScalar`: Extracts scalar value from JSONB structure
  - `cannotCastJsonbValue`: Raises error for invalid type conversions
  - `DirectFunctionCall1`: Calls a PostgreSQL function with one argument
  - `numeric_int2`: Converts numeric to smallint with range checking
  - `NumericGetDatum`: Converts numeric to datum format
  - `PG_FREE_IF_COPY`: Frees memory if input was copied
  - `PG_RETURN_DATUM`: Returns the converted value as Datum
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/jsonb.c:2073-2090`
- Part of PostgreSQL's JSONB type conversion system
- Only accepts JSONB numeric scalars (not integer scalars directly)
- Delegates actual conversion and range checking to `numeric_int2`
- Provides automatic overflow/underflow detection via `numeric_int2`
- Uses `DirectFunctionCall1` for efficient function call without SQL overhead
- Follows PostgreSQL's memory management conventions with `PG_FREE_IF_COPY`
- Error handling delegates to `cannotCastJsonbValue` for consistent error messages