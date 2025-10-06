# int64_div_fast_to_numeric

## Location
[src/backend/utils/adt/numeric.c:4320-4404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4320-L4404)

## Overview
Efficiently converts the result of val1/(10^log10val2) to PostgreSQL's Numeric data type, providing much faster division than normal numeric division.

## Definition
```c
Numeric int64_div_fast_to_numeric(int64 val1, int log10val2)
```

## Detailed Description
This function performs an optimized conversion of an integer division by a power of 10 to Numeric format. Instead of performing actual division, it manipulates the weight and scale of the numeric representation to achieve the same result much more efficiently. The function handles negative log10val2 values and uses compile-time constants to optimize multiplication operations.

The optimization works by:
1. Calculating how much to adjust the weight (w = log10val2 / DEC_DIGITS)
2. Determining any remaining division needed (m = log10val2 % DEC_DIGITS)
3. If remainder exists, multiplying the dividend by 10^(DEC_DIGITS - m) and adjusting weight
4. Setting the final weight and decimal scale appropriately

For cases where multiplication might overflow, the function falls back to either 128-bit integer arithmetic (if available) or full numeric arithmetic to maintain precision.

## Parameters / Member Variables
- `val1`: The dividend (numerator) as a 64-bit signed integer
- `log10val2`: The log base 10 of the divisor (i.e., to divide by 10^log10val2)

## Dependencies
- Functions called/Symbols referenced:
  - init_var
  - [int64_to_numericvar](int64_to_numericvar.md)
  - [int128_to_numericvar](int128_to_numericvar.md) (when HAVE_INT128 is defined)
  - [mul_var](../m/mul_var.md)
  - [make_result](../m/make_result.md)
  - [free_var](../f/free_var.md)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md)
  - StaticAssertDecl
  - DEC_DIGITS (constant)
- Called from (representative examples):
  - [time_part_common](../t/time_part_common.md)
  - [timetz_part_common](../t/timetz_part_common.md)
  - [timestamp_part_common](../t/timestamp_part_common.md)
  - [timestamptz_part_common](../t/timestamptz_part_common.md)
  - [interval_part_common](interval_part_common.md)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:4320-4404
- This function is specifically optimized for time/date calculations where division by powers of 10 is common
- Uses compile-time static arrays for power-of-10 multiplication based on DEC_DIGITS configuration
- Includes overflow protection with fallback to either 128-bit integers or full numeric arithmetic
- The optimization significantly improves performance for timestamp and interval part extraction operations
- Supports DEC_DIGITS values of 1, 2, or 4 (typical PostgreSQL configurations)

## Simplified Source

```c
Numeric
int64_div_fast_to_numeric(int64 val1, int log10val2)
{
    Numeric res;
    NumericVar result;
    int rscale;
    int w;
    int m;

    init_var(&result);

    // Calculate result scale
    rscale = log10val2 < 0 ? 0 : log10val2;

    // Determine weight adjustment and remaining division
    w = log10val2 / DEC_DIGITS;
    m = log10val2 % DEC_DIGITS;
    if (m < 0) {
        m += DEC_DIGITS;
        w--;
    }

    // Handle remaining division by multiplying dividend
    if (m > 0) {
        // Static power-of-10 tables for different DEC_DIGITS configurations
#if DEC_DIGITS == 4
        static const int pow10[] = {1, 10, 100, 1000};
#elif DEC_DIGITS == 2
        static const int pow10[] = {1, 10};
#elif DEC_DIGITS == 1
        static const int pow10[] = {1};
#endif
        int64 factor = pow10[DEC_DIGITS - m];
        int64 new_val1;

        // Check for overflow and handle accordingly
        if (unlikely(pg_mul_s64_overflow(val1, factor, &new_val1))) {
#ifdef HAVE_INT128
            // Use 128-bit arithmetic if available
            int128 tmp = (int128) val1 * (int128) factor;
            int128_to_numericvar(tmp, &result);
#else
            // Fall back to numeric arithmetic
            NumericVar tmp;
            init_var(&tmp);
            int64_to_numericvar(val1, &result);
            int64_to_numericvar(factor, &tmp);
            mul_var(&result, &tmp, &result, 0);
            free_var(&tmp);
#endif
        } else {
            int64_to_numericvar(new_val1, &result);
        }
        w++;
    } else {
        int64_to_numericvar(val1, &result);
    }

    // Set final weight and scale
    result.weight -= w;
    result.dscale = rscale;

    // Convert to external format
    res = make_result(&result);
    free_var(&result);

    return res;
}
```