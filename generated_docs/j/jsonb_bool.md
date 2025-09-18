# jsonb_bool

## Location
src/backend/utils/adt/jsonb.c: 2038 - 2051

## Overview
Converts a JSONB value to a PostgreSQL boolean, extracting the boolean value from a JSONB scalar.

## Definition
```c
Datum jsonb_bool(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_bool` function is a PostgreSQL built-in function that extracts a boolean value from a JSONB input. It validates that the JSONB contains a scalar boolean value and converts it to PostgreSQL's native boolean type. If the JSONB value is not a boolean scalar, the function raises an error through `cannotCastJsonbValue`.

The function follows PostgreSQL's standard function calling convention, taking arguments via `PG_FUNCTION_ARGS` and returning a `Datum`. It performs type checking to ensure the JSONB value can be safely cast to a boolean before extraction.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `in`: The input JSONB value to be converted to boolean

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Extracts JSONB argument from function arguments
  - [JsonbExtractScalar](../J/JsonbExtractScalar.md): Extracts scalar value from JSONB structure
  - [cannotCastJsonbValue](../c/cannotCastJsonbValue.md): Raises error for invalid type conversions
  - `PG_FREE_IF_COPY`: Frees memory if input was copied
  - `PG_RETURN_BOOL`: Returns boolean value as Datum
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/jsonb.c:2038-2051`
- Part of PostgreSQL's JSONB type conversion system
- Performs strict type checking - only accepts JSONB boolean scalars
- Follows PostgreSQL's memory management conventions with `PG_FREE_IF_COPY`
- Error handling delegates to `cannotCastJsonbValue` for consistent error messages