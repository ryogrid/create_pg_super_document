# init_var_from_num

## Location
[src/backend/utils/adt/numeric.c:7467-7483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L7467-L7483)

## Overview
Initializes a NumericVar variable from a packed database format Numeric value without copying the digits array, providing a lightweight reference for read-only operations.

## Definition

```c
static void
init_var_from_num(Numeric num, NumericVar *dest)
```
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
  - [numeric_out](../n/numeric_out.md) (numeric to string conversion)
  - [numeric_is_integral](../n/numeric_is_integral.md) (integrality testing)
  - [numeric_add_opt_error](../n/numeric_add_opt_error.md) (arithmetic addition)
  - [numeric_sub_opt_error](../n/numeric_sub_opt_error.md) (arithmetic subtraction)
  - [numeric_mul_opt_error](../n/numeric_mul_opt_error.md) (arithmetic multiplication)
  - [numeric_div_opt_error](../n/numeric_div_opt_error.md) (arithmetic division)
  - [numeric_power](../n/numeric_power.md) (exponentiation operations)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c module
- No memory allocation is performed - the digits array is shared with the source Numeric
- The buf field is explicitly set to NULL to indicate no separate allocation
- [free_var](../f/free_var.md)() should not be called on variables initialized this way
- Suitable for read-only access or when the variable will be completely overwritten
- Used extensively throughout numeric operations for performance optimization
- The caller must ensure the source Numeric remains valid for the lifetime of the NumericVar