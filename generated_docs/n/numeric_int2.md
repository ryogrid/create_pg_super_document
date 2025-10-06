# numeric_int2

## Location
[src/backend/utils/adt/numeric.c:4569-4608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4569-L4608)

## Overview
Converts a PostgreSQL numeric value to a 16-bit signed integer (smallint), performing range validation and error handling for special numeric values.

## Definition

```c
Datum
numeric_int2(PG_FUNCTION_ARGS)
```
## Detailed Description
The `numeric_int2` function converts a PostgreSQL `Numeric` type to a 16-bit signed integer (`int16`). It handles the conversion by first checking for special numeric values (NaN and infinity), then converting to an intermediate 64-bit integer representation, and finally performing range validation to ensure the result fits within the smallint range (-32768 to 32767). The function follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function parameter macro that provides access to function arguments
  - Argument 0: `Numeric` input value to be converted to smallint

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC`: Retrieves the numeric argument from function parameters
  - `NUMERIC_IS_SPECIAL`: Checks if the numeric value is special (NaN or infinity)
  - `NUMERIC_IS_NAN`: Checks specifically for NaN values
  - [init_var_from_num](../i/init_var_from_num.md): Initializes a NumericVar from a Numeric value
  - [numericvar_to_int64](numericvar_to_int64.md): Converts NumericVar to 64-bit integer
  - `PG_RETURN_INT16`: Returns the 16-bit integer result
- Called from (representative examples):
  - [jsonb_int2](../j/jsonb_int2.md): JSONB to smallint conversion

## Notes and Other Information
- Throws `ERRCODE_FEATURE_NOT_SUPPORTED` error for NaN and infinity inputs
- Throws `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE` error when the numeric value exceeds smallint range
- Uses intermediate 64-bit integer conversion to ensure precision during range checking
- [Range](../R/Range.md) validation uses `PG_INT16_MIN` and `PG_INT16_MAX` constants for boundary checking
- Part of PostgreSQL's numeric type conversion system in `src/backend/utils/adt/numeric.c`

## Simplified Source

```c
Datum numeric_int2(PG_FUNCTION_ARGS) {
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar x;
    int64 val;

    // Check for special values (NaN, infinity) and reject them
    if (NUMERIC_IS_SPECIAL(num)) {
        if (NUMERIC_IS_NAN(num))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("cannot convert NaN to smallint")));
        else
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("cannot convert infinity to smallint")));
    }

    // Convert numeric to internal variable format, then to int64
    init_var_from_num(num, &x);
    if (!numericvar_to_int64(&x, &val))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("smallint out of range")));

    // Check if value fits in smallint range (-32768 to 32767)
    if (val < PG_INT16_MIN || val > PG_INT16_MAX)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("smallint out of range")));

    // Cast to int16 and return
    return PG_RETURN_INT16((int16) val);
}
```