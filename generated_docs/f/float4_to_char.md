# float4_to_char

## Location
[src/backend/utils/adt/formatting.c:6727-6828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6727-L6828)

## Overview
Converts a single-precision floating-point number (float4) to its formatted text representation according to a specified format pattern.

## Definition


## Detailed Description
The `float4_to_char` function formats a single-precision floating-point value into a text string according to a format specification. It handles the unique characteristics of floating-point numbers including special values (NaN, infinity) and precision limitations. The function supports three main formatting categories:

1. **Roman numeral formatting**: Rounds the float to the nearest integer and converts to Roman numerals
2. **Scientific notation (EEEE format)**: Uses printf-style scientific notation with proper handling of special values
3. **Standard decimal formatting**: Manages floating-point precision limitations using FLT_DIG and adjusts decimal places accordingly

The function includes special handling for floating-point precision by limiting the total significant digits to FLT_DIG to avoid displaying meaningless precision artifacts.

## Parameters / Member Variables
- `PG_GETARG_FLOAT4(0)`: The single-precision floating-point value to be formatted
- `PG_GETARG_TEXT_PP(1)`: The format pattern string specifying how the number should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4, PG_GETARG_TEXT_PP (PostgreSQL function argument macros)
  - float4, NUMDesc, FormatNode (data types and formatting structures)
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish (formatting preparation/cleanup macros)
  - IS_ROMAN, IS_EEEE, IS_MULTI (format type checking macros)
  - [int_to_roman](../i/int_to_roman.md) (Roman numeral conversion)
  - isnan, isinf (floating-point special value detection)
  - rint (rounding to nearest integer)
  - [psprintf](../p/psprintf.md) (PostgreSQL's sprintf wrapper)
  - [fill_str](fill_str.md) (string padding utility)
  - FLT_DIG (floating-point precision constant)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Special handling for NaN and infinity values in scientific notation by filling with '#' characters
- Precision management: limits total digits to FLT_DIG (typically 6-7 digits) to avoid displaying false precision
- Automatic adjustment of decimal places based on the number of digits before the decimal point
- Roman numeral conversion uses rounding to convert float to integer
- Scientific notation replaces leading '+' with space for alignment consistency
- Overflow conditions in standard formatting are handled by filling with '#' characters
- Part of PostgreSQL's comprehensive text formatting system in src/backend/utils/adt/formatting.c