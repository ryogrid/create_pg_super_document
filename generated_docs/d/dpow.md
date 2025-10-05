# dpow

## Location
[src/backend/utils/adt/float.c:1482-1636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1482-L1636)

## Overview
The dpow function computes the power operation (arg1^arg2) for double-precision floating-point numbers with comprehensive domain validation, special case handling, and error detection.

## Definition
```c
Datum dpow(PG_FUNCTION_ARGS)
```

## Detailed Description
The dpow function implements the mathematical power operation for double-precision floating-point numbers (float8). This is one of the most complex mathematical functions in PostgreSQL due to the numerous special cases and edge conditions that must be handled correctly. The function implements:

1. **NaN Handling**: Follows POSIX specifications for NaN ^ 0 = 1, 1 ^ NaN = 1, with all other NaN cases returning NaN
2. **Domain Validation**: 
   - Prevents 0 ^ (negative number) with appropriate SQL error
   - Prevents (negative number) ^ (non-integer) with appropriate SQL error
3. **Infinity Handling**: Comprehensive logic for when either argument is infinite
4. **Overflow/Underflow Detection**: Multiple layers of error detection beyond standard pow() function
5. **Platform Compatibility**: Works around known bugs in older glibc versions

The function uses the standard C library pow() function but adds extensive PostgreSQL-specific validation and error handling.

## Parameters / Member Variables
- `arg1`: The base value (double-precision floating-point number)
- `arg2`: The exponent value (double-precision floating-point number)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 arguments)
  - isnan (standard C math library function to check for NaN)
  - [get_float8_nan](../g/get_float8_nan.md) (PostgreSQL function to get NaN value)
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function) 
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
  - floor (standard C math library function)
  - isinf (standard C math library function to check for infinity)
  - fabs (standard C math library function for absolute value)
  - pow (standard C math library function)
  - unlikely (compiler hint macro)
  - [float_overflow_error](../f/float_overflow_error.md) (PostgreSQL float overflow error handler)
  - [float_underflow_error](../f/float_underflow_error.md) (PostgreSQL float underflow error handler)
  - PG_RETURN_FLOAT8 (macro to return float8 result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's floating-point arithmetic operations
- Located in src/backend/utils/adt/float.c which contains various floating-point utility functions
- One of the most complex mathematical functions due to numerous special cases
- Uses specific SQL error code ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION for domain errors
- Implements workarounds for known platform-specific bugs in older glibc versions
- Handles all infinity and NaN cases explicitly rather than relying on platform pow() behavior
- Includes extensive commentary explaining the rationale for various edge case handling
- Follows standard PostgreSQL function conventions for SQL-callable functions

## Simplified Source

```c
Datum dpow(PG_FUNCTION_ARGS) {
    // Extract base and exponent arguments
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 arg2 = PG_GETARG_FLOAT8(1);
    float8 result;

    // Handle NaN cases per POSIX (NaN^0 = 1, 1^NaN = 1)
    if (isnan(arg1)) {
        if (isnan(arg2) || arg2 != 0.0)
            PG_RETURN_FLOAT8(get_float8_nan());
        PG_RETURN_FLOAT8(1.0);
    }
    if (isnan(arg2)) {
        if (arg1 != 1.0)
            PG_RETURN_FLOAT8(get_float8_nan());
        PG_RETURN_FLOAT8(1.0);
    }

    // Domain validation
    if (arg1 == 0 && arg2 < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                       errmsg("zero raised to a negative power is undefined")));
    if (arg1 < 0 && floor(arg2) != arg2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                       errmsg("a negative number raised to a non-integer power yields a complex result")));

    // Handle infinity cases
    if (isinf(arg2)) {
        float8 absx = fabs(arg1);
        if (absx == 1.0)
            result = 1.0;
        else if (arg2 > 0.0)
            result = (absx > 1.0) ? arg2 : 0.0;
        else
            result = (absx > 1.0) ? 0.0 : -arg2;
    }
    else if (isinf(arg1)) {
        // Handle +/-Inf base cases
        if (arg2 == 0.0)
            result = 1.0;
        else if (arg1 > 0.0)
            result = (arg2 > 0.0) ? arg1 : 0.0;
        else {
            // Handle sign for negative infinity base
            float8 halfy = arg2 / 2;
            bool yisoddinteger = (floor(halfy) != halfy);
            if (arg2 > 0.0)
                result = yisoddinteger ? arg1 : -arg1;
            else
                result = yisoddinteger ? -0.0 : 0.0;
        }
    }
    else {
        // Standard pow() with comprehensive error checking
        errno = 0;
        result = pow(arg1, arg2);

        // Handle various error conditions and platform bugs
        if (errno == EDOM || isnan(result)) {
            // Handle large exponent case
            if (arg1 == 0.0)
                result = 0.0;
            else {
                float8 absx = fabs(arg1);
                if (absx == 1.0)
                    result = 1.0;
                else if (arg2 >= 0.0 ? (absx > 1.0) : (absx < 1.0))
                    float_overflow_error();
                else
                    float_underflow_error();
            }
        }
        else if (errno == ERANGE) {
            if (result != 0.0)
                float_overflow_error();
            else
                float_underflow_error();
        }
        else {
            // Final overflow/underflow checks
            if (unlikely(isinf(result)))
                float_overflow_error();
            if (unlikely(result == 0.0) && arg1 != 0.0)
                float_underflow_error();
        }
    }

    PG_RETURN_FLOAT8(result);
}
```