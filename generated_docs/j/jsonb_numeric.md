# jsonb_numeric

## Location
[src/backend/utils/adt/jsonb.c:2052-2072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L2052-L2072)

## Overview
Converts a JSONB value to a PostgreSQL numeric type, extracting the numeric value from a JSONB scalar with proper memory management.

## Definition
```c
Datum jsonb_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_numeric` function extracts a numeric value from a JSONB input and converts it to PostgreSQL's native `Numeric` type. It validates that the JSONB contains a scalar numeric value and creates a proper copy of the numeric data since the original value points into the JSONB body structure. If the JSONB value is not a numeric scalar, the function raises an error through `cannotCastJsonbValue`.

The function includes important memory management considerations, as it must create a copy of the numeric value rather than returning a direct reference to data within the JSONB structure. This ensures proper memory lifecycle management in PostgreSQL's execution context.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `in`: The input JSONB value to be converted to numeric
- `retValue`: Local variable holding the copied numeric value to return

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Extracts JSONB argument from function arguments
  - [JsonbExtractScalar](../J/JsonbExtractScalar.md): Extracts scalar value from JSONB structure
  - [cannotCastJsonbValue](../c/cannotCastJsonbValue.md): Raises error for invalid type conversions
  - [DatumGetNumericCopy](../D/DatumGetNumericCopy.md): Creates a copy of a numeric datum
  - [NumericGetDatum](../N/NumericGetDatum.md): Converts numeric to datum format
  - `PG_FREE_IF_COPY`: Frees memory if input was copied
  - `PG_RETURN_NUMERIC`: Returns numeric value as Datum
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/jsonb.c:2052-2072`
- Part of PostgreSQL's JSONB type conversion system
- Performs strict type checking - only accepts JSONB numeric scalars
- Critical memory management: creates a copy since `v.val.numeric` points into JSONB body
- Uses `DatumGetNumericCopy` to ensure proper memory allocation for the returned value
- Follows PostgreSQL's memory management conventions with `PG_FREE_IF_COPY`
- Error handling delegates to `cannotCastJsonbValue` for consistent error messages