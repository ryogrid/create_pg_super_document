# numericvar_to_uint64

## Location
[src/backend/utils/adt/numeric.c:8167-8239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8167-L8239)

## Overview
The `numericvar_to_uint64` function converts a PostgreSQL numeric variable to a 64-bit unsigned integer, performing rounding to the nearest integer and checking for overflow and negative value conditions.

## Definition

```c
struct the result */
	digits = rounded.digits;
```
## Detailed Description
This function performs a safe conversion from PostgreSQL's arbitrary-precision numeric representation to a 64-bit unsigned integer:

1. **Rounding**: Creates a copy of the input and rounds it to the nearest integer (scale 0)
2. **Zero Handling**: Special case optimization for zero values
3. **Negative Check**: Rejects negative values since uint64 cannot represent them
4. **Digit Processing**: Processes the numeric representation digit by digit, respecting the weight (position) of each digit
5. **Overflow Detection**: Uses PostgreSQL's overflow-safe unsigned arithmetic functions to detect when the result would exceed uint64 range
6. **Accumulation**: Builds the result by multiplying previous value by NBASE and adding each digit
7. **Memory Management**: Properly cleans up temporary variables

Unlike the signed int64 version, this function uses straightforward positive accumulation since all values are non-negative.

## Parameters / Member Variables
- `var`: Pointer to the source NumericVar structure containing the value to convert
- `result`: Pointer to uint64 where the converted value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `init_var`: Initialize temporary numeric variable
  - [set_var_from_var](../s/set_var_from_var.md): Copy numeric variable content
  - [round_var](../r/round_var.md): Round to specified decimal places
  - [strip_var](../s/strip_var.md): Remove leading/trailing zeros
  - [free_var](../f/free_var.md): Free numeric variable memory
  - [pg_mul_u64_overflow](../p/pg_mul_u64_overflow.md): Overflow-safe 64-bit unsigned multiplication
  - [pg_add_u64_overflow](../p/pg_add_u64_overflow.md): Overflow-safe 64-bit unsigned addition
  - `NBASE`: Numeric digit base constant
  - `NUMERIC_NEG`: Constant for negative sign
  - `NumericDigit`: Type for individual digits

- Called from (representative examples):
  - `NUMERIC_CAN_BE_SHORT`: Short numeric validation
  - [numeric_pg_lsn](numeric_pg_lsn.md): PostgreSQL LSN conversion

## Notes and Other Information
- Returns `true` on successful conversion, `false` on overflow or negative input (no exceptions thrown)
- Explicitly rejects negative values before processing since uint64 is unsigned
- Uses overflow-safe unsigned arithmetic to prevent undefined behavior during conversion
- Simpler than the int64 version since it doesn't need to handle the INT64_MIN edge case
- Strips leading zeros before processing to optimize performance
- The weight field determines how many digits appear before the decimal point
- Properly manages memory for the temporary rounded variable
- Supports conversion of large numeric values that fit within uint64 range
- Uses addition instead of subtraction for accumulation since all values are positive

## Simplified Source

```c
static bool numericvar_to_uint64(const NumericVar *var, uint64 *result) {
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

    // Reject negative values
    if (rounded.sign == NUMERIC_NEG) {
        free_var(&rounded);
        return false;
    }

    // Build result from digits, checking for overflow
    uint64 val = rounded.digits[0];

    for (int i = 1; i <= rounded.weight; i++) {
        // Check for multiplication overflow
        if (unlikely(pg_mul_u64_overflow(val, NBASE, &val))) {
            free_var(&rounded);
            return false;
        }

        // Add next digit if it exists
        if (i < rounded.ndigits) {
            if (unlikely(pg_add_u64_overflow(val, rounded.digits[i], &val))) {
                free_var(&rounded);
                return false;
            }
        }
    }

    free_var(&rounded);
    *result = val;
    return true;
}
```