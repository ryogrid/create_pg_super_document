# int4_to_char

## Location
src/backend/utils/adt/formatting.c: 6527 - 6620

## Overview
A PostgreSQL built-in function that converts a 32-bit integer value to a formatted text string using a specified format pattern.

## Definition


## Detailed Description
This function implements PostgreSQL's TO_CHAR functionality specifically for 32-bit integer values. It takes an int32 value and a format pattern, then produces a formatted string representation according to the pattern specifications.

The function handles several formatting modes:
- Roman numeral conversion (RN/rn): Converts the integer directly to Roman numerals using int_to_roman
- Scientific notation (EEEE): Uses psprintf to format in exponential notation, converting to float8 for precision, and replaces leading '+' with space for alignment
- Standard formatting: Applies multiplicative scaling using integer arithmetic and pow(), handles decimal padding with zeros, and manages overflow conditions

Key features include sign extraction and handling, prefix padding calculation, overflow protection by filling with '#' characters, and post-decimal zero padding when decimal places are specified in the format.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro, which provides access to:
  - : Input 32-bit integer value to format
  - : Format pattern string specifying desired output format

## Dependencies
- Functions called/Symbols referenced:
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish, int_to_roman, int4out
  - DirectFunctionCall1, DatumGetCString, Int32GetDatum
  - psprintf, fill_str, pow (mathematical power function)
  - IS_ROMAN, IS_EEEE, IS_MULTI (formatting condition macros)
- Called from (representative examples):
  - This is a SQL-callable function, typically not called directly from C code

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as TO_CHAR(integer, text)
- Optimized for integer values, avoiding floating-point precision issues where possible
- Roman numeral conversion works directly with the integer value without rounding
- Scientific notation temporarily converts to float8 but this won't lose precision for int32 range
- Multiplicative formatting (V pattern) uses integer arithmetic with pow() for scaling
- Handles post-decimal formatting by padding with zeros rather than actual decimal computation
- Overflow conditions are indicated by filling output with '#' characters
- Memory management includes proper allocation for various output buffer sizes
- Integrates with PostgreSQL's comprehensive formatting system using shared macros
- Returns a PostgreSQL text datum suitable for SQL result sets
- Part of PostgreSQL's family of type-specific formatting functions in formatting.c