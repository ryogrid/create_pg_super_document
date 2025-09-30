# apply_typmod

## Location
[src/backend/utils/adt/numeric.c:7923-8007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L7923-L8007)

## Overview
The `apply_typmod` function performs bounds checking and rounding on a numeric value according to the specified type modifier (typmod), ensuring the value conforms to the precision and scale constraints defined by the numeric type.

## Definition

```c
static bool
apply_typmod(NumericVar *var, int32 typmod, Node *escontext)
```
## Detailed Description
This function is responsible for enforcing numeric type constraints by:

1. **Validation**: First checks if the provided typmod is valid using `is_valid_numeric_typmod`
2. **Precision/Scale Extraction**: Extracts precision and scale values from the typmod
3. **Rounding**: Rounds the numeric value to the target scale using `round_var`
4. **Overflow Detection**: Performs sophisticated overflow checking by calculating the actual number of significant digits, accounting for leading zeros and the internal digit representation
5. **Error Handling**: Uses soft error handling via `escontext` to allow callers to handle errors gracefully

The function only applies to normal finite values and uses PostgreSQL's internal numeric representation with configurable digit packing (DEC_DIGITS can be 1, 2, or 4).

## Parameters / Member Variables
- `var`: Pointer to the NumericVar structure containing the numeric value to be modified
- `typmod`: 32-bit type modifier encoding precision and scale information  
- `escontext`: Error handling context node that determines whether errors are thrown or returned

## Dependencies
- Functions called/Symbols referenced:
  - [is_valid_numeric_typmod](../i/is_valid_numeric_typmod.md): Validates the typmod parameter
  - [numeric_typmod_precision](../n/numeric_typmod_precision.md): Extracts precision from typmod
  - [numeric_typmod_scale](../n/numeric_typmod_scale.md): Extracts scale from typmod  
  - [round_var](../r/round_var.md): Rounds the numeric value to specified scale
  - `ereturn`: Soft error return mechanism
  - `DEC_DIGITS`: Macro defining digits per internal digit unit
  - `NumericDigit`: Type for individual numeric digits

- Called from (representative examples):
  - `NUMERIC_CAN_BE_SHORT`: Short numeric value validation
  - [numeric_in](../n/numeric_in.md): Text input parsing
  - [numeric_recv](../n/numeric_recv.md): Binary input parsing
  - [numeric](../n/numeric.md): Type coercion function

## Notes and Other Information
- Returns `true` on success, `false` on failure when using soft error handling
- The overflow detection algorithm accounts for PostgreSQL's packed decimal digit representation
- Leading zeros are stripped during the overflow calculation to determine the true significant digit count
- The function handles different DEC_DIGITS configurations (1, 2, or 4 digits per storage unit)
- Type modifier validation ensures the function gracefully handles invalid typmod values

## Simplified Source

```c
static bool
apply_typmod(NumericVar *var, int32 typmod, Node *escontext)
{
    int precision, scale, maxdigits;
    int actual_digits, i;

    // Skip if typmod is invalid
    if (!is_valid_numeric_typmod(typmod))
        return true;

    // Extract precision and scale from typmod
    precision = numeric_typmod_precision(typmod);
    scale = numeric_typmod_scale(typmod);
    maxdigits = precision - scale;  // Max digits before decimal point

    // Round to target scale
    round_var(var, scale);
    if (var->dscale < 0)
        var->dscale = 0;

    // Check for overflow by counting actual significant digits
    actual_digits = (var->weight + 1) * DEC_DIGITS;

    if (actual_digits > maxdigits) {
        // Find first non-zero digit to get true count
        for (i = 0; i < var->ndigits; i++) {
            NumericDigit digit = var->digits[i];

            if (digit) {
                // Adjust for leading zeros within the digit
                if (digit < 10)
                    actual_digits -= (DEC_DIGITS - 1);
                else if (digit < 100 && DEC_DIGITS >= 3)
                    actual_digits -= (DEC_DIGITS - 2);
                else if (digit < 1000 && DEC_DIGITS >= 4)
                    actual_digits -= (DEC_DIGITS - 3);

                // Check if still overflows
                if (actual_digits > maxdigits) {
                    return ereturn(escontext, false,
                                 (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                  errmsg("numeric field overflow")));
                }
                break;
            }
            actual_digits -= DEC_DIGITS;
        }
    }

    return true;
}
```