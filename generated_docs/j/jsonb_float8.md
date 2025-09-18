# jsonb_float8

## Location
src/backend/utils/adt/jsonb.c: 2145 - 2165

## Overview
Converts a JSONB value to a PostgreSQL float8 (double precision) data type, extracting numeric scalar values from JSONB format.

## Definition
```c
Datum jsonb_float8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs type conversion from JSONB to float8 (double-precision floating point). It extracts scalar numeric values from a JSONB input and converts them to PostgreSQL's double precision data type using the underlying numeric conversion infrastructure. The function validates that the JSONB value contains a numeric scalar before attempting conversion, throwing an error for incompatible types.

## Parameters / Member Variables
- Input: JSONB value via `PG_GETARG_JSONB_P(0)` - the JSONB value to convert
- Output: `Datum` - the converted float8 value wrapped as a PostgreSQL Datum

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P` - extracts JSONB argument from function call
  - `JsonbExtractScalar` - extracts scalar value from JSONB root
  - `cannotCastJsonbValue` - throws error for invalid type conversions
  - `DirectFunctionCall1` - calls another PostgreSQL function directly
  - `numeric_float8` - converts numeric to float8
  - `NumericGetDatum` - wraps numeric value as Datum
  - `PG_FREE_IF_COPY` - frees memory if input was copied
  - `PG_RETURN_DATUM` - returns the result Datum
- Called from (representative examples):
  - No direct references found (likely called via SQL cast operations)

## Notes and Other Information
- Only accepts JSONB values containing numeric scalars (jbvNumeric type)
- Throws an error with message "double precision" if the JSONB value cannot be converted
- Uses PostgreSQL's standard numeric conversion pathway for consistent behavior
- Part of the JSONB type casting infrastructure in PostgreSQL
- Companion function to jsonb_float4, providing higher precision conversion
- Located in src/backend/utils/adt/jsonb.c:2145-2165