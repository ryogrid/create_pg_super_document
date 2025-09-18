# gcd_var

## Location
src/backend/utils/adt/numeric.c: 10008 - 10077

## Overview
The `gcd_var` function calculates the greatest common divisor (GCD) of two numeric values using the Euclidean algorithm, implementing this mathematical operation for PostgreSQL's `NumericVar` data type.

## Definition
```c
static void gcd_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
```

## Detailed Description
This function implements the greatest common divisor calculation for PostgreSQL's variable-precision numeric data type using the classical Euclidean algorithm. The GCD is the largest positive number that divides both input numbers without remainder. The function includes several optimizations:

1. **Input arrangement**: Ensures the larger absolute value is processed first to reduce iterations
2. **Early termination**: Handles cases where inputs have equal absolute values or one input is zero
3. **Interruption safety**: Allows long-running calculations to be interrupted
4. **Sign handling**: Always returns a positive result regardless of input signs

The algorithm:
1. Determines the maximum decimal scale for the result
2. Arranges inputs so var1 has the greater absolute value
3. Handles special cases (equal values, zero input)
4. Applies the Euclidean algorithm iteratively until remainder is zero
5. Sets the result as positive with appropriate decimal scale

## Parameters / Member Variables
- `var1`: First input `NumericVar` for GCD calculation
- `var2`: Second input `NumericVar` for GCD calculation  
- `result`: Output `NumericVar` where the GCD result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `cmp_abs`: Compare absolute values of two `NumericVar` values
  - `set_var_from_var`: Copy one `NumericVar` to another
  - `init_var`: Initialize a new `NumericVar` structure
  - `mod_var`: Calculate modulo of two `NumericVar` values
  - `free_var`: Free memory associated with a `NumericVar`
  - `CHECK_FOR_INTERRUPTS`: Macro to allow interruption of long operations
  - `NUMERIC_POS`: Constant representing positive sign
  - `Max`: Macro to get maximum of two values

- Called from (representative examples):
  - `numeric_gcd`: SQL-callable GCD function wrapper
  - `numeric_lcm`: Used internally for least common multiple calculation

## Notes and Other Information
- This is a static function internal to the numeric data type implementation
- Uses the Euclidean algorithm, which is efficient and mathematically robust
- The result is always positive, following mathematical convention for GCD
- Includes interrupt checking for very large numbers that could cause long computation times
- Part of PostgreSQL's extended mathematical function set for precise arithmetic
- The decimal scale of the result is set to the maximum of the input scales
- Performance optimization by arranging inputs to minimize modulo operations