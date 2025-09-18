# jsonb_int4

## Location
src/backend/utils/adt/jsonb.c: 2091 - 2108

## Overview
Converts a JSONB numeric value to a PostgreSQL integer (int4) type, using numeric-to-integer conversion with range checking.

## Definition
```c
Datum jsonb_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_int4` function extracts a numeric value from a JSONB input and converts it to PostgreSQL's standard `integer` (int4) type. It first validates that the JSONB contains a scalar numeric value, then delegates the actual conversion to the `numeric_int4` function via `DirectFunctionCall1`. This approach ensures that proper range checking and conversion logic is applied, as `numeric_int4` handles bounds checking for the integer type (-2,147,483,648 to 2,147,483,647).

The function follows a two-step conversion process: first extracting the numeric value from JSONB, then converting that numeric to integer using PostgreSQL's existing numeric conversion infrastructure. This design provides consistent behavior with other numeric conversion functions in PostgreSQL.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `in`: The input JSONB value to be converted to integer
- `retValue`: Local variable holding the converted integer value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Extracts JSONB argument from function arguments
  - [JsonbExtractScalar](../J/JsonbExtractScalar.md): Extracts scalar value from JSONB structure
  - [cannotCastJsonbValue](../c/cannotCastJsonbValue.md): Raises error for invalid type conversions
  - `DirectFunctionCall1`: Calls a PostgreSQL function with one argument
  - [numeric_int4](../n/numeric_int4.md): Converts numeric to integer with range checking
  - [NumericGetDatum](../N/NumericGetDatum.md): Converts numeric to datum format
  - `PG_FREE_IF_COPY`: Frees memory if input was copied
  - `PG_RETURN_DATUM`: Returns the converted value as Datum
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/jsonb.c:2091-2108`
- Part of PostgreSQL's JSONB type conversion system
- Only accepts JSONB numeric scalars (not integer scalars directly)
- Delegates actual conversion and range checking to `numeric_int4`
- Provides automatic overflow/underflow detection via `numeric_int4`
- Uses `DirectFunctionCall1` for efficient function call without SQL overhead
- Follows PostgreSQL's memory management conventions with `PG_FREE_IF_COPY`
- Error handling delegates to `cannotCastJsonbValue` for consistent error messages
- Most commonly used integer conversion for JSONB values in PostgreSQL applications