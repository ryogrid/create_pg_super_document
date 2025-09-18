# sqrt_var

## Location
[src/backend/utils/adt/numeric.c:10078-10557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L10078-L10557)

## Overview
The `sqrt_var` function computes the square root of a numeric value using the Karatsuba Square Root algorithm, providing high-precision square root calculation for PostgreSQL's `NumericVar` data type.

## Definition
```c
static void sqrt_var(const NumericVar *arg, NumericVar *result, int rscale)
```

## Detailed Description
This function implements a sophisticated square root algorithm based on the Karatsuba Square Root method, which efficiently computes square roots of arbitrary-precision numbers. The algorithm is implemented iteratively rather than recursively for better performance and memory management.

Key features:
1. **Input validation**: Handles zero input and rejects negative numbers with appropriate SQL error codes
2. **Multi-stage computation**: Uses different precision levels (int64, int128 if available, then NumericVar) for optimal performance
3. **Karatsuba algorithm**: Recursively breaks down the problem into smaller square root computations
4. **Newton's method**: Uses Newton-Raphson iteration for initial approximation refinement
5. **Precision control**: Allows negative rscale for rounding before the decimal point

The algorithm treats inputs as integers during computation and works by repeatedly applying the recursive Karatsuba formula:
- SqrtRem(n = a3*b³ + a2*b² + a1*b + a0)
- Computes square root and remainder through divide-and-conquer

## Parameters / Member Variables
- `arg`: Input `NumericVar` containing the numeric value to compute square root of
- `result`: Output `NumericVar` where the square root result will be stored
- `rscale`: Number of decimal places in the result (can be negative for rounding before decimal point)

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_var](../c/cmp_var.md): Compare two `NumericVar` values
  - [zero_var](../z/zero_var.md): Set a `NumericVar` to zero
  - `init_var`: Initialize `NumericVar` structures
  - [set_var_from_var](set_var_from_var.md): Copy one `NumericVar` to another
  - [add_var](../a/add_var.md), `sub_var`, `mul_var`: Arithmetic operations
  - [div_mod_var](../d/div_mod_var.md): Division with remainder
  - [round_var](../r/round_var.md): Round to specified decimal places
  - [strip_var](strip_var.md): Remove leading/trailing zeros
  - [alloc_var](../a/alloc_var.md): Allocate memory for `NumericVar`
  - [free_var](../f/free_var.md): Free memory associated with `NumericVar`
  - [int64_to_numericvar](../i/int64_to_numericvar.md), `int128_to_numericvar`: Convert integers to `NumericVar`
  - Various constants: `const_zero`, `const_one`, `NUMERIC_POS`, `NUMERIC_NEG`

- Called from (representative examples):
  - [numeric_sqrt](../n/numeric_sqrt.md): SQL-callable square root function
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md): Used in standard deviation calculations
  - [ln_var](../l/ln_var.md): Used in natural logarithm computations

## Notes and Other Information
- This is a static function internal to the numeric data type implementation
- Implements the advanced Karatsuba Square Root algorithm for optimal performance on large numbers
- Uses multiple precision levels: starts with double precision for small inputs, then int64, int128 (if available), and finally full NumericVar arithmetic
- Includes comprehensive input validation following SQL2003 standards for power functions
- The algorithm guarantees convergence and handles edge cases like perfect squares
- Performance is optimized by minimizing expensive numeric operations through staged computation
- Supports very high precision calculations limited only by available memory
- The implementation is interrupt-safe for long-running computations