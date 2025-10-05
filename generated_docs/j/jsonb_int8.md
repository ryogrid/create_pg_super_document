# jsonb_int8

## Location
[src/backend/utils/adt/jsonb.c:2109-2126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L2109-L2126)

## Overview
Converts a JSONB numeric value to a PostgreSQL bigint (int8) type, using numeric-to-bigint conversion with range checking.

## Definition
```c
Datum jsonb_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_int8` function extracts a numeric value from a JSONB input and converts it to PostgreSQL's `bigint` (int8) type. It first validates that the JSONB contains a scalar numeric value, then delegates the actual conversion to the `numeric_int8` function via `DirectFunctionCall1`. This approach ensures that proper range checking and conversion logic is applied, as `numeric_int8` handles bounds checking for the bigint type (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807).

The function follows a two-step conversion process: first extracting the numeric value from JSONB, then converting that numeric to bigint using PostgreSQL's existing numeric conversion infrastructure. This design provides the widest range of integer values available in PostgreSQL for JSONB numeric conversions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `in`: The input JSONB value to be converted to bigint
- `retValue`: Local variable holding the converted bigint value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Extracts JSONB argument from function arguments
  - [JsonbExtractScalar](../J/JsonbExtractScalar.md): Extracts scalar value from JSONB structure
  - [cannotCastJsonbValue](../c/cannotCastJsonbValue.md): Raises error for invalid type conversions
  - `DirectFunctionCall1`: Calls a PostgreSQL function with one argument
  - [numeric_int8](../n/numeric_int8.md): Converts numeric to bigint with range checking
  - [NumericGetDatum](../N/NumericGetDatum.md): Converts numeric to datum format
  - `PG_FREE_IF_COPY`: Frees memory if input was copied
  - `PG_RETURN_DATUM`: Returns the converted value as Datum
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/jsonb.c:2109-2126`
- Part of PostgreSQL's JSONB type conversion system
- Only accepts JSONB numeric scalars (not integer scalars directly)
- Delegates actual conversion and range checking to `numeric_int8`
- Provides automatic overflow/underflow detection via `numeric_int8`
- Supports the largest integer range available in PostgreSQL (64-bit signed)
- Uses `DirectFunctionCall1` for efficient function call without SQL overhead
- Follows PostgreSQL's memory management conventions with `PG_FREE_IF_COPY`
- Error handling delegates to `cannotCastJsonbValue` for consistent error messages
- Preferred conversion for large integer values that may exceed int4 range

## Simplified Source

```c
Datum
jsonb_int8(PG_FUNCTION_ARGS)
{
    Jsonb *in = PG_GETARG_JSONB_P(0);
    JsonbValue v;

    // Extract scalar value and validate it's numeric
    if (!JsonbExtractScalar(&in->root, &v) || v.type != jbvNumeric)
        cannotCastJsonbValue(v.type, "bigint");

    // Convert numeric to int8 with range checking
    Datum retValue = DirectFunctionCall1(numeric_int8,
                                        NumericGetDatum(v.val.numeric));

    PG_FREE_IF_COPY(in, 0);
    PG_RETURN_DATUM(retValue);
}
```