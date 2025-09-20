# trunc_var

## Location
[src/backend/utils/adt/numeric.c:11873-11934](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11873-L11934)

## Overview
Truncates a NumericVar value towards zero at a specified number of decimal digits after the decimal point, providing precise control over numeric precision without rounding.

## Definition

```c
static void
trunc_var(NumericVar *var, int rscale)
```
## Detailed Description
The  function implements decimal truncation for PostgreSQL's numeric type by simply cutting off digits beyond the specified precision without any rounding. Unlike , this function always truncates towards zero regardless of the value of discarded digits. It supports negative rscale values for truncation before the decimal point. The function works efficiently with the internal NBASE digit representation and handles within-digit truncation when the target precision falls in the middle of a NBASE digit.

## Parameters / Member Variables
- : Pointer to NumericVar structure to be truncated (modified in place)
- : Target number of decimal digits after decimal point (can be negative for truncation before decimal point)

## Dependencies
- Functions called/Symbols referenced:
  - DEC_DIGITS (decimal digits per NBASE digit constant)
  - NUMERIC_POS (positive sign constant)
  - NumericDigit (type for individual digits)
  - round_powers (power-of-10 lookup table for DEC_DIGITS=4)
- Called from (representative examples):
  - [numeric_recv](../n/numeric_recv.md) (receiving numeric values from binary format)
  - [numeric_trunc](../n/numeric_trunc.md) (SQL TRUNC function)
  - [div_var](../d/div_var.md), div_var_fast, div_var_int, div_var_int64 (division operations)
  - [ceil_var](../c/ceil_var.md), floor_var (ceiling and floor functions)

## Notes and Other Information
- Always truncates towards zero - no rounding occurs regardless of discarded digit values
- Supports rscale < 0 for truncation before decimal point (e.g., rscale=-2 truncates to hundreds place)
- Uses conditional compilation for different DEC_DIGITS values (1, 2, 4)
- For very negative rscale values that eliminate all significant digits, the result becomes 0
- More efficient than round_var since no carry propagation is needed
- When di <= 0, all digits are eliminated and the result is set to positive zero
- Uses modular arithmetic for within-digit truncation when DEC_DIGITS > 1
- The dscale field is immediately set to the target rscale value