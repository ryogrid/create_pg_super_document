# pg_strtof

## Location
src/port/strtof.c: 30 - 89

## Overview
A PostgreSQL-specific wrapper for the strtof() function that provides correct overflow/underflow handling and avoids double-rounding problems on platforms with buggy strtof() implementations.

## Definition
```c
float pg_strtof(const char *nptr, char **endptr);
```

## Detailed Description
The pg_strtof function serves as a replacement for the standard strtof() function on platforms where the system's strtof() implementation is buggy. Specifically, this function addresses issues found on Cygwin and MinGW where strtof() is implemented as a simple cast from strtod(), leading to double-rounding problems.

The function works by:
1. First attempting to parse the string using the system's strtof()
2. If the result appears problematic (subnormal values that might be affected by double-rounding), it validates the result by comparing with strtod()
3. For subnormal values, it uses the more accurate strtod() result cast to float
4. Properly handles error conditions and maintains errno semantics

This wrapper ensures proper overflow/underflow detection while avoiding the precision issues that can occur with naive double-to-float conversions.

## Parameters / Member Variables
- `nptr`: Pointer to the null-terminated string to be parsed as a floating-point number
- `endptr`: Optional pointer to a char pointer that will be set to point to the first character after the parsed number (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strtof (system function)
  - strtod (system function)
  - isnan (math function)
  - isinf (math function)
- Called from (representative examples):
  - Replaced via macro definition when HAVE_BUGGY_STRTOF is defined

## Notes and Other Information
- This function is only compiled and used on platforms where HAVE_BUGGY_STRTOF is defined
- On affected platforms, the standard strtof() function is replaced via a macro definition
- The function specifically handles subnormal floating-point values that may be incorrectly rounded by buggy strtof() implementations
- Maintains full compatibility with the standard strtof() interface including errno handling
- Located in src/port/strtof.c as part of PostgreSQL's portability layer