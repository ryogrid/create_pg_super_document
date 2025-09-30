# make_result_opt_error

## Location
[src/backend/utils/adt/numeric.c:7798-7906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L7798-L7906)

## Overview
Converts a NumericVar structure to the packed database numeric format with optional error handling for overflow conditions, supporting both normal values and special cases like NaN and Infinity.

## Definition
```c
static Numeric make_result_opt_error(const NumericVar *var, bool *have_error)
```

## Detailed Description
This function creates a packed Numeric value from a NumericVar structure, handling the conversion from the internal computational format to the storage format used in the database. The function includes comprehensive error handling for overflow conditions and supports all numeric types including special values (NaN, positive infinity, negative infinity).

The function implements several optimizations including leading and trailing zero truncation, automatic selection between short and long numeric formats based on the magnitude of weight and dscale values, and proper handling of zero values by normalizing them to positive zero with weight 0.

When overflow occurs (weight or dscale values exceed the limits of int16 fields), the function can either return NULL and set an error flag, or throw an exception depending on the have_error parameter.

## Parameters / Member Variables
- `var`: Pointer to the source NumericVar structure containing the value to be converted (const, read-only)
- `have_error`: Optional pointer to a boolean flag that will be set to true if overflow occurs (can be NULL for exception-throwing behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - SET_VARSIZE (setting variable-length structure size)
  - memcpy (copying digit data)
  - [dump_numeric](../d/dump_numeric.md) (debugging function)
  - elog, ereport (error reporting)
  - NUMERIC_CAN_BE_SHORT (macro for format selection)
  - Various NUMERIC_* constants and macros for bit manipulation
- Called from (representative examples):
  - [numeric_in](../n/numeric_in.md)
  - [numeric_add_opt_error](../n/numeric_add_opt_error.md)
  - [numeric_sub_opt_error](../n/numeric_sub_opt_error.md)
  - [numeric_mul_opt_error](../n/numeric_mul_opt_error.md)
  - [numeric_div_opt_error](../n/numeric_div_opt_error.md)
  - [numeric_mod_opt_error](../n/numeric_mod_opt_error.md)
  - [make_result](make_result.md)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c file
- The function automatically chooses between short and long numeric formats to optimize storage space
- Leading and trailing zeros are stripped to minimize storage requirements
- Special values (NaN, ±Infinity) are handled through a separate code path using only the header
- Zero values are normalized to positive zero with weight 0 for consistency
- The function validates special value signs to prevent corruption of reserved bits
- When have_error is NULL, overflow conditions result in exceptions; when provided, they result in graceful error handling
- The packed format uses either 16-bit or 32-bit fields depending on the chosen format (short vs long)
- Overflow detection ensures data integrity by verifying that weight and dscale values fit in their target field sizes

## Simplified Source

```c
static Numeric
make_result_opt_error(const NumericVar *var, bool *have_error)
{
    Numeric result;
    NumericDigit *digits = var->digits;
    int weight = var->weight;
    int sign = var->sign;
    int n;
    Size len;

    if (have_error)
        *have_error = false;

    // Handle special values (NaN, ±Infinity)
    if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL) {
        // Validate special value
        if (!(sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF))
            elog(ERROR, "invalid numeric sign value 0x%x", sign);

        result = (Numeric) palloc(NUMERIC_HDRSZ_SHORT);
        SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
        result->choice.n_header = sign;
        return result;
    }

    n = var->ndigits;

    // Remove leading zeros
    while (n > 0 && *digits == 0) {
        digits++;
        weight--;
        n--;
    }

    // Remove trailing zeros
    while (n > 0 && digits[n - 1] == 0)
        n--;

    // Normalize zero values
    if (n == 0) {
        weight = 0;
        sign = NUMERIC_POS;
    }

    // Choose short or long format based on value ranges
    if (NUMERIC_CAN_BE_SHORT(var->dscale, weight)) {
        // Short format: pack scale and weight into header
        len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
        result = (Numeric) palloc(len);
        SET_VARSIZE(result, len);
        result->choice.n_short.n_header =
            (sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT)
            | (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
            | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
            | (weight & NUMERIC_SHORT_WEIGHT_MASK);
    } else {
        // Long format: separate fields for scale and weight
        len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
        result = (Numeric) palloc(len);
        SET_VARSIZE(result, len);
        result->choice.n_long.n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);
        result->choice.n_long.n_weight = weight;
    }

    // Copy digit data
    if (n > 0)
        memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

    // Check for overflow in target format
    if (NUMERIC_WEIGHT(result) != weight || NUMERIC_DSCALE(result) != var->dscale) {
        if (have_error) {
            *have_error = true;
            return NULL;
        } else {
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("value overflows numeric format")));
        }
    }

    return result;
}
```