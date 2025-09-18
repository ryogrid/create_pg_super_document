# jsonb_float4

## Location
src/backend/utils/adt/jsonb.c: 2127 - 2144

## Overview
Converts a JSONB value to a PostgreSQL float4 (real) data type, extracting numeric scalar values from JSONB format.

## Definition
```c
Datum jsonb_float4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs type conversion from JSONB to float4 (single-precision floating point). It extracts scalar numeric values from a JSONB input and converts them to PostgreSQL's real data type using the underlying numeric conversion infrastructure. The function validates that the JSONB value contains a numeric scalar before attempting conversion, throwing an error for incompatible types.

## Parameters / Member Variables
- Input: JSONB value via `PG_GETARG_JSONB_P(0)` - the JSONB value to convert
- Output: `Datum` - the converted float4 value wrapped as a PostgreSQL Datum

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P` - extracts JSONB argument from function call
  - `JsonbExtractScalar` - extracts scalar value from JSONB root
  - `cannotCastJsonbValue` - throws error for invalid type conversions  
  - `DirectFunctionCall1` - calls another PostgreSQL function directly
  - `numeric_float4` - converts numeric to float4
  - `NumericGetDatum` - wraps numeric value as Datum
  - `PG_FREE_IF_COPY` - frees memory if input was copied
  - `PG_RETURN_DATUM` - returns the result Datum
- Called from (representative examples):
  - No direct references found (likely called via SQL cast operations)

## Notes and Other Information
- Only accepts JSONB values containing numeric scalars (jbvNumeric type)
- Throws an error with message "real" if the JSONB value cannot be converted
- Uses PostgreSQL's standard numeric conversion pathway for consistent behavior
- Part of the JSONB type casting infrastructure in PostgreSQL
- Located in src/backend/utils/adt/jsonb.c:2127-2144