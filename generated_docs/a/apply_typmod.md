# apply_typmod

## Location
src/backend/utils/adt/numeric.c: 7923 - 8007

## Overview
The `apply_typmod` function performs bounds checking and rounding on a numeric value according to the specified type modifier (typmod), ensuring the value conforms to the precision and scale constraints defined by the numeric type.

## Definition


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