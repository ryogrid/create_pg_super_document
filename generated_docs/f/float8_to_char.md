# float8_to_char

## Location
[src/backend/utils/adt/formatting.c:6829-6924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6829-L6924)

## Overview
Converts a double-precision floating-point number (float8) to its formatted text representation according to a specified format pattern.

## Definition


## Detailed Description
The `float8_to_char` function formats a double-precision floating-point value into a text string according to a format specification. It handles the unique characteristics of double-precision floating-point numbers including special values (NaN, infinity) and precision limitations. The function supports three main formatting categories:

1. **Roman numeral formatting**: Rounds the double to the nearest integer and converts to Roman numerals
2. **Scientific notation (EEEE format)**: Uses printf-style scientific notation with proper handling of special values
3. **Standard decimal formatting**: Manages floating-point precision limitations using DBL_DIG and adjusts decimal places accordingly

The function includes special handling for double-precision floating-point precision by limiting the total significant digits to DBL_DIG (typically 15-17 digits) to avoid displaying meaningless precision artifacts. This is similar to float4_to_char but with higher precision limits appropriate for double-precision values.

## Parameters / Member Variables
- `PG_GETARG_FLOAT8(0)`: The double-precision floating-point value to be formatted
- `PG_GETARG_TEXT_PP(1)`: The format pattern string specifying how the number should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8, PG_GETARG_TEXT_PP (PostgreSQL function argument macros)
  - float8, NUMDesc, FormatNode (data types and formatting structures)
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish (formatting preparation/cleanup macros)
  - IS_ROMAN, IS_EEEE, IS_MULTI (format type checking macros)
  - [int_to_roman](../i/int_to_roman.md) (Roman numeral conversion)
  - isnan, isinf (floating-point special value detection)
  - rint (rounding to nearest integer)
  - [psprintf](../p/psprintf.md) (PostgreSQL's sprintf wrapper)
  - [fill_str](fill_str.md) (string padding utility)
  - pow (power function for multiplier calculations)
  - fabs (absolute value function)
  - DBL_DIG (double-precision floating-point precision constant)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Special handling for NaN and infinity values in scientific notation by filling with '#' characters
- Precision management: limits total digits to DBL_DIG (typically 15-17 digits) to avoid displaying false precision
- Automatic adjustment of decimal places based on the number of digits before the decimal point
- Roman numeral conversion uses rounding to convert double to integer
- Scientific notation replaces leading '+' with space for alignment consistency
- Overflow conditions in standard formatting are handled by filling with '#' characters
- Higher precision handling compared to float4_to_char due to double-precision characteristics
- Part of PostgreSQL's comprehensive text formatting system in src/backend/utils/adt/formatting.c
- Uses double arithmetic for multiplier calculations to maintain precision