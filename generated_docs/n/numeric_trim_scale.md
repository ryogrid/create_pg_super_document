# numeric_trim_scale

## Location
[src/backend/utils/adt/numeric.c:4223-4243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4223-L4243)

## Overview
Reduces the scale of a numeric value to its minimum required representation without loss of precision by removing trailing zeros.

## Definition

```c
Datum
numeric_trim_scale(PG_FUNCTION_ARGS)
```
## Detailed Description
The `numeric_trim_scale` function is a PostgreSQL built-in function that creates a new numeric value with trailing zeros removed from the fractional part. It calculates the minimum scale needed to represent the value accurately and returns a new numeric with that reduced scale. For special values (NaN, infinity), it returns a duplicate of the original value. This function is useful for normalizing numeric values and reducing storage overhead.

## Parameters / Member Variables
- Input: A single numeric value accessed via `PG_GETARG_NUMERIC(0)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC` - Extracts numeric argument from function call
  - `NUMERIC_IS_SPECIAL` - Checks if numeric value is special (NaN, infinity)
  - [duplicate_numeric](../d/duplicate_numeric.md) - Creates a copy of special numeric values
  - [init_var_from_num](../i/init_var_from_num.md) - Converts external Numeric to internal NumericVar
  - [get_min_scale](../g/get_min_scale.md) - Calculates the minimum required scale
  - [make_result](../m/make_result.md) - Converts NumericVar back to external Numeric format
  - [free_var](../f/free_var.md) - Releases memory allocated for NumericVar
  - `PG_RETURN_NUMERIC` - Returns numeric result
- Called from (representative examples):
  - Currently no direct references found in the analyzed codebase

## Notes and Other Information
- For special values, returns a duplicate rather than attempting scale reduction
- The function creates a new numeric value rather than modifying the input
- Useful for normalizing numeric representations (e.g., 1.2000 becomes 1.2)
- Can help reduce storage requirements for numeric values with unnecessary trailing zeros
- Properly manages memory by freeing temporary NumericVar structures
- Part of PostgreSQL's numeric data type utility functions
- Located in src/backend/utils/adt/numeric.c:4223-4243

## Simplified Source

```c
Datum
numeric_trim_scale(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar result;

    // Return copy of special values unchanged
    if (NUMERIC_IS_SPECIAL(num))
        PG_RETURN_NUMERIC(duplicate_numeric(num));

    // Convert to internal format, set scale to minimum, convert back
    init_var_from_num(num, &result);
    result.dscale = get_min_scale(&result);
    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
```