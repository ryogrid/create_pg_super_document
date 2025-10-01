# get_float8_infinity

## Location
[src/interfaces/ecpg/ecpglib/data.c:80-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/data.c#L80-L89)

## Overview
A utility function that returns the IEEE 754 representation of positive infinity as a double-precision floating-point value, with fallback implementation for systems that don't support the INFINITY macro.

## Definition
```c
static double get_float8_infinity(void)
```

## Detailed Description
The `get_float8_infinity` function provides a portable way to obtain the IEEE 754 positive infinity value for double-precision floating-point operations. This function is part of PostgreSQL's ECPG library and is used in data type conversion and special value handling.

The function uses conditional compilation to provide the most appropriate implementation:
- **When INFINITY is available**: Uses the standard C99 `INFINITY` macro cast to double
- **When INFINITY is not available**: Falls back to computing infinity by multiplying `HUGE_VAL * HUGE_VAL`, which mathematically results in positive infinity

This approach ensures compatibility across different C standard library implementations and compilers while maintaining IEEE 754 compliance.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - INFINITY (C99 macro, conditionally used)
  - HUGE_VAL (C standard library constant)
- Called from (representative examples):
  - [check_special_value](../c/check_special_value.md) (in ECPG library)
  - [float8in_internal](../f/float8in_internal.md) (in backend float parsing)
  - Various mathematical and geometric functions throughout PostgreSQL

## Notes and Other Information
- This is a static function, only accessible within data.c in the ECPG library context
- The function provides IEEE 754 positive infinity, which is a special floating-point value
- The fallback method (`HUGE_VAL * HUGE_VAL`) is a common technique for generating infinity on older systems
- Used extensively throughout PostgreSQL for handling special floating-point values in mathematical computations, geometric operations, and statistical functions
- Critical for proper handling of overflow conditions and infinite values in SQL operations

## Simplified Source

```c
static double get_float8_infinity(void)
{
    // Use C99 INFINITY if available, otherwise compute it
#ifdef INFINITY
    return (double) INFINITY;
#else
    // Fallback: HUGE_VAL * HUGE_VAL = positive infinity
    return (double) (HUGE_VAL * HUGE_VAL);
#endif
}
```