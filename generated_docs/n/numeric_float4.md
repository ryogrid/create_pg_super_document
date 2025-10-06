# numeric_float4

## Location
[src/backend/utils/adt/numeric.c:4738-4765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4738-L4765)

## Overview
Converts a PostgreSQL numeric value to a 32-bit floating-point (float4) value, handling special numeric values like infinity and NaN appropriately.

## Definition

```c
Datum
numeric_float4(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs type conversion from PostgreSQL's arbitrary precision numeric type to a 32-bit floating-point value. It first checks for special numeric values (positive infinity, negative infinity, and NaN) and handles them by returning the corresponding float4 special values. For regular numeric values, it uses an intermediate string conversion approach: first converting the numeric to its string representation using , then parsing that string as a float4 using . This two-step conversion ensures proper handling of precision and rounding according to PostgreSQL's established conversion rules.

## Parameters / Member Variables
- Input parameter accessed via : The numeric value to be converted to float4

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if numeric has special value
  -  - Check for positive infinity
  -  - Check for negative infinity
  -  - Get float4 infinity value
  -  - Get float4 NaN value
  -  - Convert numeric to string representation
  -  - Parse string as float4 value
  -  - Direct function call interface
  -  - Extract C string from Datum
  -  - Convert C string to Datum
  -  - Convert Numeric to Datum
- Called from (representative examples):
  -  - JSONB to float4 conversion

## Notes and Other Information
- Uses PostgreSQL's function call interface (, , etc.)
- Handles special numeric values (infinity, NaN) explicitly before attempting conversion
- Uses string-based intermediate conversion rather than direct binary conversion for compatibility
- Memory management: properly frees the temporary string using
- Located in src/backend/utils/adt/numeric.c:4738-4765

## Simplified Source

```c
Datum numeric_float4(PG_FUNCTION_ARGS) {
    Numeric num = PG_GETARG_NUMERIC(0);
    char *tmp;
    Datum result;

    // Handle special numeric values (infinity, NaN)
    if (NUMERIC_IS_SPECIAL(num)) {
        if (NUMERIC_IS_PINF(num))
            PG_RETURN_FLOAT4(get_float4_infinity());
        else if (NUMERIC_IS_NINF(num))
            PG_RETURN_FLOAT4(-get_float4_infinity());
        else
            PG_RETURN_FLOAT4(get_float4_nan());
    }

    // Convert numeric to string, then string to float4 for accuracy
    tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));
    result = DirectFunctionCall1(float4in, CStringGetDatum(tmp));

    // Clean up temporary string
    pfree(tmp);

    return PG_RETURN_DATUM(result);
}
```