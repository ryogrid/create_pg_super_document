# power_ten_int

## Location
[src/backend/utils/adt/numeric.c:11314-11338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11314-L11338)

## Overview
Raises ten to the power of an integer exponent and stores the result in a NumericVar structure, used for exact decimal arithmetic in PostgreSQL's numeric type implementation.

## Definition

```c
struct the result directly, starting from 10^0 = 1 */
	set_var_from_var(&const_one, result);
```
## Detailed Description
This function computes 10^exp where exp is an integer, storing the exact result in the provided NumericVar structure. Unlike power_var_int(), this function performs no overflow/underflow checking or rounding, making it suitable for internal numeric calculations where exact powers of ten are needed. The function constructs the result directly by starting from 10^0 = 1 and then adjusting the weight and scale to represent the power of ten exactly in PostgreSQL's internal numeric format.

The implementation efficiently handles both positive and negative exponents by:
- For positive exponents: setting the weight to position the decimal point correctly
- For negative exponents: setting the dscale (decimal scale) to represent fractional powers of ten
- Using base-NBASE arithmetic to minimize the number of digits needed

## Parameters / Member Variables
- : The integer exponent to raise 10 to the power of (can be positive, negative, or zero)
- : Pointer to NumericVar structure where the computed result (10^exp) will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [set_var_from_var](../s/set_var_from_var.md) (copies const_one to initialize result)
  - const_one (constant representing the value 1)
  - DEC_DIGITS (constant defining digits per NBASE unit)
- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT (numeric optimization function)
  - [get_str_from_var_sci](../g/get_str_from_var_sci.md) (scientific notation formatting)

## Notes and Other Information
- This is a static function internal to numeric.c, not exposed in the public API
- No overflow/underflow checking is performed, assuming valid input ranges
- The function efficiently represents powers of ten using PostgreSQL's base-NBASE numeric representation
- Negative exponents result in fractional values (0.1, 0.01, etc.) represented with appropriate dscale
- Used primarily in numeric formatting and internal calculations where exact powers of ten are required

## Simplified Source

```c
static void power_ten_int(int exp, NumericVar *result) {
    // Start with 1 (10^0 = 1)
    set_var_from_var(&const_one, result);

    // Set decimal scale for negative exponents (fractional results)
    result->dscale = exp < 0 ? -exp : 0;

    // Calculate base weight and remaining exponent
    if (exp >= 0)
        result->weight = exp / DEC_DIGITS;
    else
        result->weight = (exp + 1) / DEC_DIGITS - 1;

    exp -= result->weight * DEC_DIGITS;

    // Adjust the single digit by remaining powers of 10
    while (exp-- > 0)
        result->digits[0] *= 10;
}
```