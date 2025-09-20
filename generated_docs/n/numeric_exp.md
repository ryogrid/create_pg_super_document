# numeric_exp

## Location
[src/backend/utils/adt/numeric.c:3764-3830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3764-L3830)

## Overview
Computes the exponential function (e^x) for a numeric value with intelligent scale determination and special value handling.

## Definition

```c
Datum
numeric_exp(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function calculates e raised to the power of a numeric input value. It implements proper mathematical semantics for special values: exp(-∞) returns zero per POSIX standards, while NaN and +∞ are preserved. The function uses a sophisticated scale calculation that estimates the decimal weight of the result using the mathematical relationship log10(result) = x * log10(e), where the constant 0.434294481903252 represents log10(e).

The implementation converts the input to a double for scale estimation while using high-precision numeric arithmetic for the actual calculation via . Result scale bounds are carefully managed to prevent integer overflow and ensure reasonable precision.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing the input numeric value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extract numeric argument from function args
  - NUMERIC_IS_SPECIAL: Check if numeric is NaN or infinity  
  - NUMERIC_IS_NINF: Check if numeric is negative infinity
  - [make_result](../m/make_result.md): Convert NumericVar to Numeric result
  - const_zero: Constant zero NumericVar for exp(-∞) case
  - [duplicate_numeric](../d/duplicate_numeric.md): Create copy of numeric value
  - [init_var_from_num](../i/init_var_from_num.md): Initialize NumericVar from Numeric
  - init_var: Initialize empty NumericVar
  - [numericvar_to_double_no_overflow](numericvar_to_double_no_overflow.md): Convert to double for scale estimation
  - [exp_var](../e/exp_var.md): Core exponential calculation function
  - [free_var](../f/free_var.md): Free NumericVar memory
  - PG_RETURN_NUMERIC: Return numeric result
- Called from (representative examples):
  - SQL exp() function calls
  - Numeric exponential expressions

## Notes and Other Information
- Returns zero for negative infinity inputs per POSIX specification
- Uses log10(e) ≈ 0.434294481903252 for result scale estimation
- [Result](../R/Result.md) scale is bounded by  to prevent overflow
- Maintains at least  significant digits in result
- Uses  for high-precision exponential computation
- Located in 