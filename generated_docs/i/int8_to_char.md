# int8_to_char

## Location
[src/backend/utils/adt/formatting.c:6621-6726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6621-L6726)

## Overview
Converts a 64-bit integer (int8) to its formatted text representation according to a specified format pattern.

## Definition


## Detailed Description
The  function formats a 64-bit integer value into a text string according to a format specification. It supports various formatting options including Roman numerals, scientific notation, decimal formatting with padding, and precision control. The function handles three main formatting categories:

1. **Roman numeral formatting**: Converts the int8 to int4 and then to Roman numerals (with precision limitations)
2. **Scientific notation (EEEE format)**: Uses numeric representation to maintain precision and avoid floating-point conversion
3. **Standard decimal formatting**: Supports padding, decimal places, sign handling, and overflow indication

The function uses PostgreSQL's standard formatting infrastructure with NUMDesc and FormatNode structures to parse and apply the format specification.

## Parameters / Member Variables
- : The 64-bit integer value to be formatted
- : The format pattern string specifying how the number should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64, PG_GETARG_TEXT_PP (PostgreSQL function argument macros)
  - NUMDesc, FormatNode (formatting structure types)
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish (formatting preparation/cleanup macros)
  - IS_ROMAN, IS_EEEE, IS_MULTI (format type checking macros)
  - [int_to_roman](int_to_roman.md) (Roman numeral conversion)
  - [int84](int84.md), int8out, int8mul (int8 type conversion functions)
  - [numeric_out_sci](../n/numeric_out_sci.md), int64_to_numeric (numeric precision handling)
  - DirectFunctionCall1, DirectFunctionCall2 (PostgreSQL function call utilities)
  - [fill_str](../f/fill_str.md) (string padding utility)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Roman numeral conversion is limited by the int84 conversion, potentially losing precision for very large int8 values
- Scientific notation formatting preserves full precision by avoiding floating-point conversion and using the numeric type instead
- The function handles sign display explicitly, adding space padding for positive numbers in scientific notation to maintain alignment
- Overflow conditions are handled by filling the output with '#' characters when the number exceeds the specified format width
- Part of PostgreSQL's comprehensive text formatting system in src/backend/utils/adt/formatting.c