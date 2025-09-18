# exp_var

## Location
src/backend/utils/adt/numeric.c: 10558 - 10686

## Overview
The `exp_var` function computes the exponential function (e^x) for a numeric value using Taylor series expansion with range reduction techniques, providing high-precision exponential calculation for PostgreSQL's `NumericVar` data type.

## Definition
```c
static void exp_var(const NumericVar *arg, NumericVar *result, int rscale)
```

## Detailed Description
This function implements the mathematical exponential function e^x using a sophisticated algorithm that combines Taylor series expansion with range reduction for optimal precision and performance. The implementation includes several key optimizations:

1. **Overflow protection**: Guards against overflow and underflow by checking argument bounds
2. **Range reduction**: Reduces the argument to the range [-0.01, 0.01] by repeated division by 2^ndiv2
3. **Taylor series**: Uses the series exp(x) = 1 + x + x²/2! + x³/3! + ... for computation
4. **Precision management**: Dynamically adjusts working precision based on expected result magnitude
5. **Compensation**: Reverses the range reduction by squaring the result ndiv2 times

The algorithm works by:
1. Converting input to double to estimate result magnitude and detect overflow
2. Reducing argument range through division by powers of 2
3. Computing Taylor series expansion with appropriate precision
4. Compensating for range reduction through repeated squaring
5. Rounding to the requested precision

## Parameters / Member Variables
- `arg`: Input `NumericVar` containing the exponent value (x in e^x)
- `result`: Output `NumericVar` where the exponential result will be stored
- `rscale`: Number of fractional digits in the result

## Dependencies
- Functions called/Symbols referenced:
  - `init_var`: Initialize `NumericVar` structures
  - [set_var_from_var](../s/set_var_from_var.md): Copy one `NumericVar` to another
  - [numericvar_to_double_no_overflow](../n/numericvar_to_double_no_overflow.md): Convert `NumericVar` to double for estimation
  - [zero_var](../z/zero_var.md): Set a `NumericVar` to zero
  - [div_var_int](../d/div_var_int.md): Divide `NumericVar` by integer
  - [add_var](../a/add_var.md): Add two `NumericVar` values
  - [mul_var](../m/mul_var.md): Multiply two `NumericVar` values
  - [round_var](../r/round_var.md): Round to specified decimal places
  - [free_var](../f/free_var.md): Free memory associated with `NumericVar`
  - Constants: `const_one`, `NUMERIC_MAX_RESULT_SCALE`, `NUMERIC_MIN_DISPLAY_SCALE`, `DEC_DIGITS`

- Called from (representative examples):
  - [numeric_exp](../n/numeric_exp.md): SQL-callable exponential function
  - [power_var](../p/power_var.md): Used in power function computations (for non-integer exponents)

## Notes and Other Information
- This is a static function internal to the numeric data type implementation
- Uses Taylor series expansion which converges rapidly for small arguments (hence the range reduction)
- Includes comprehensive overflow detection following PostgreSQL's numeric limits
- The range reduction technique (dividing by 2^n then squaring n times) maintains precision while improving convergence
- Performance is optimized by adjusting working precision dynamically as computation proceeds
- Handles edge cases like very large positive arguments (overflow) and very large negative arguments (underflow to zero)
- The algorithm is numerically stable and provides results accurate to the specified precision
- Used as a building block for other transcendental functions in PostgreSQL's numeric system
- The convergence criterion stops when Taylor series terms become negligible relative to the working precision