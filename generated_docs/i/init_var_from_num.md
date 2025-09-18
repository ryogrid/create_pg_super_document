# init_var_from_num

## Location
src/backend/utils/adt/numeric.c: 7467 - 7483

## Overview
Initializes a NumericVar variable from a packed database format Numeric value without copying the digits array, providing a lightweight reference for read-only operations.

## Definition


## Detailed Description
This static function provides an efficient way to initialize a NumericVar from a Numeric value when the digits array does not need to be modified. Unlike set_var_from_num(), this function does not allocate memory or copy the digits array - instead, it directly points the NumericVar's digits pointer to the original Numeric's digits buffer.

This optimization saves memory allocation cycles and is suitable for read-only operations or when the NumericVar will be used as a destination for calculations that will replace its contents entirely. The function sets the buf pointer to NULL to indicate that the digits array was not allocated and should not be freed.

**IMPORTANT CAUTION**: The digits buffer must not be modified when using this initialization method, as changes would propagate back to the original Numeric value, potentially corrupting stored data.

## Parameters / Member Variables
- `num`: Source Numeric value in packed database format to reference
- `dest`: Destination NumericVar pointer that will be initialized to reference the source data

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_NDIGITS (macro to get number of digits)
  - NUMERIC_WEIGHT (macro to extract weight component)
  - NUMERIC_SIGN (macro to extract sign component)
  - NUMERIC_DSCALE (macro to extract display scale)
  - NUMERIC_DIGITS (macro to get digit array pointer)
- Called from (representative examples):
  - numeric_out (numeric to string conversion)
  - numeric_is_integral (integrality testing)
  - numeric_add_opt_error (arithmetic addition)
  - numeric_sub_opt_error (arithmetic subtraction)
  - numeric_mul_opt_error (arithmetic multiplication)
  - numeric_div_opt_error (arithmetic division)
  - numeric_power (exponentiation operations)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c module
- No memory allocation is performed - the digits array is shared with the source Numeric
- The buf field is explicitly set to NULL to indicate no separate allocation
- free_var() should not be called on variables initialized this way
- Suitable for read-only access or when the variable will be completely overwritten
- Used extensively throughout numeric operations for performance optimization
- The caller must ensure the source Numeric remains valid for the lifetime of the NumericVar