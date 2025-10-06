# numeric_is_integral

## Location
[src/backend/utils/adt/numeric.c:871-904](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L871-L904)

## Overview
A static utility function that determines whether a Numeric value represents an integral (whole) number without fractional parts.

## Definition
```c
static bool numeric_is_integral(Numeric num)
```

## Detailed Description
The `numeric_is_integral` function tests whether a Numeric value is an integer (has no fractional component). The function handles both regular numeric values and special values according to these rules:

1. **NaN values**: Returns `false` - NaN is not considered integral
2. **Infinity values**: Returns `true` - both positive and negative infinity are considered integral
3. **Regular values**: Returns `true` if there are no significant digits to the right of the decimal point

The function works by examining the internal structure of the Numeric value, specifically looking at the number of digits (`ndigits`) and the position of the decimal point (`weight`). A value is integral if all its significant digits are to the left of or at the decimal point position.

## Parameters / Member Variables
- `num`: The Numeric value to test for integral status

## Dependencies
- Functions called/Symbols referenced:
  - `NUMERIC_IS_SPECIAL`: Checks if the value is NaN or infinity
  - `NUMERIC_IS_NAN`: Specifically tests for NaN values
  - [init_var_from_num](../i/init_var_from_num.md): Converts Numeric to NumericVar for analysis
- Called from (representative examples):
  - [numeric_power](numeric_power.md): Power function uses this to optimize integer exponent calculations

## Notes and Other Information
- This is a static function, meaning it's only accessible within the numeric.c source file
- The logic `arg.ndigits == 0 || arg.ndigits <= arg.weight + 1` determines integrality:
  - `arg.ndigits == 0`: Value is exactly zero
  - `arg.ndigits <= arg.weight + 1`: All digits are at or left of decimal point
- Used primarily in mathematical operations like `numeric_power` where integer vs. fractional handling differs
- Infinity values are pragmatically considered integral for mathematical operations
- The function provides an efficient way to determine integrality without converting to string representation

## Simplified Source

```c
static bool numeric_is_integral(Numeric num) {
    NumericVar arg;

    // Handle special values: NaN and infinity
    if (NUMERIC_IS_SPECIAL(num)) {
        if (NUMERIC_IS_NAN(num))
            return false;  // NaN is not integral
        return true;       // Infinity is considered integral
    }

    // Convert to internal representation
    init_var_from_num(num, &arg);

    // Check if all digits are at or left of decimal point
    // ndigits == 0: value is zero (integral)
    // ndigits <= weight + 1: all digits are before/at decimal point
    return (arg.ndigits == 0 || arg.ndigits <= arg.weight + 1);
}
```