# ceil_var

## Location
[src/backend/utils/adt/numeric.c:9961-9984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L9961-L9984)

## Overview
The `ceil_var` function returns the smallest integer greater than or equal to the given numeric value, implementing the mathematical ceiling function for PostgreSQL's `NumericVar` data type.

## Definition
```c
static void ceil_var(const NumericVar *var, NumericVar *result)
```

## Detailed Description
This function implements the ceiling operation for PostgreSQL's variable-precision numeric data type. It takes a `NumericVar` input and computes the smallest integer that is greater than or equal to the input value. The function works by first truncating the input to remove any fractional part, then adding 1 if the original number was positive and had a fractional component. The result is stored in the provided result parameter.

The algorithm:
1. Creates a temporary `NumericVar` and copies the input value
2. Truncates the temporary value to zero decimal places (removes fractional part)
3. If the original number was positive and not already an integer, adds 1 to the truncated value
4. Copies the final result to the output parameter

## Parameters / Member Variables
- `var`: Input `NumericVar` containing the numeric value to apply ceiling operation to
- `result`: Output `NumericVar` where the ceiling result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `init_var`: Initialize a new `NumericVar` structure
  - [set_var_from_var](../s/set_var_from_var.md): Copy one `NumericVar` to another
  - [trunc_var](../t/trunc_var.md): Truncate a numeric value to specified decimal places
  - [cmp_var](cmp_var.md): Compare two `NumericVar` values
  - [add_var](../a/add_var.md): Add two `NumericVar` values
  - [free_var](../f/free_var.md): Free memory associated with a `NumericVar`
  - `NUMERIC_POS`: Constant representing positive sign
  - `const_one`: Predefined `NumericVar` constant representing value 1

- Called from (representative examples):
  - `[numeric_ceil](../n/numeric_ceil.md)`: SQL-callable ceiling function wrapper

## Notes and Other Information
- This is a static function internal to the numeric data type implementation
- The function handles the sign correctly - negative numbers are truncated toward zero
- Uses temporary variable management to avoid modifying the input parameter
- Part of PostgreSQL's high-precision arithmetic system that avoids floating-point limitations

## Simplified Source

```c
static void ceil_var(const NumericVar *var, NumericVar *result)
{
    NumericVar tmp;

    // Initialize temporary variable and copy input
    init_var(&tmp);
    set_var_from_var(var, &tmp);

    // Truncate to remove fractional part
    trunc_var(&tmp, 0);

    // If positive and had fractional part, add 1
    if (var->sign == NUMERIC_POS && cmp_var(var, &tmp) != 0)
        add_var(&tmp, &const_one, &tmp);

    // Store result and cleanup
    set_var_from_var(&tmp, result);
    free_var(&tmp);
}
```