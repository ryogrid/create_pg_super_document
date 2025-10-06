# numericvar_to_int128

## Location
[src/backend/utils/adt/numeric.c:8240-8310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8240-L8310)

## Overview
Converts a PostgreSQL NumericVar to a 128-bit signed integer, performing rounding if necessary and detecting overflow conditions.

## Definition

```c
struct the result */
	digits = rounded.digits;
```
## Detailed Description
This function converts a PostgreSQL numeric value represented as a NumericVar structure to a 128-bit signed integer (int128). The conversion process includes automatic rounding to the nearest integer and comprehensive overflow detection. The function is designed to handle the full range of int128 values, including the special case of INT128_MIN which requires careful overflow checking due to two's complement arithmetic properties.

The conversion algorithm first rounds the input to the nearest integer, then constructs the result by processing the numeric digits from most significant to least significant, multiplying by the numeric base (NBASE) at each step. Overflow detection is performed at each multiplication step to ensure the result fits within the int128 range.

## Parameters / Member Variables
- : Pointer to the input NumericVar structure containing the numeric value to convert
- : Pointer to int128 where the converted result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - init_var: Initialize a NumericVar structure
  - [set_var_from_var](../s/set_var_from_var.md): Copy one NumericVar to another
  - [round_var](../r/round_var.md): Round a NumericVar to specified decimal places
  - [strip_var](../s/strip_var.md): Remove leading/trailing zeros from NumericVar
  - [free_var](../f/free_var.md): Free memory allocated for NumericVar
  - NUMERIC_NEG: Constant indicating negative sign
  - NBASE: Numeric base constant (10000)
  - NumericDigit: Type for numeric digit storage

- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT: Numeric optimization function
  - [numeric_poly_deserialize](numeric_poly_deserialize.md): Deserialization function for numeric polynomials
  - [int8_avg_deserialize](../i/int8_avg_deserialize.md): Deserialization function for int8 averages

## Notes and Other Information
- Returns true on successful conversion, false on overflow
- Handles the special case of INT128_MIN overflow detection using the property that -val == val only for INT128_MIN in two's complement arithmetic
- Input is automatically rounded to the nearest integer before conversion
- The function assumes weight >= 0 and ndigits <= weight + 1 after stripping
- Memory management is handled properly with free_var() calls on all exit paths
- Zero input is handled as a special case for efficiency

## Simplified Source

```c
static bool numericvar_to_int128(const NumericVar *var, int128 *result) {
    NumericVar rounded;

    // Round to nearest integer
    init_var(&rounded);
    set_var_from_var(var, &rounded);
    round_var(&rounded, 0);

    // Handle zero case
    strip_var(&rounded);
    if (rounded.ndigits == 0) {
        *result = 0;
        free_var(&rounded);
        return true;
    }

    // Build result from digits with overflow checking
    bool neg = (rounded.sign == NUMERIC_NEG);
    int128 val = rounded.digits[0];

    for (int i = 1; i <= rounded.weight; i++) {
        int128 oldval = val;
        val *= NBASE;
        if (i < rounded.ndigits)
            val += rounded.digits[i];

        // Check for overflow (special handling for INT128_MIN)
        if ((val / NBASE) != oldval) {
            // INT128_MIN is the only value where -val == val
            if (!neg || (-val) != val || val == 0 || oldval < 0) {
                free_var(&rounded);
                return false;
            }
        }
    }

    free_var(&rounded);
    *result = neg ? -val : val;
    return true;
}
```