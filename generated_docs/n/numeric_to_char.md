# numeric_to_char

## Location
[src/backend/utils/adt/formatting.c:6402-6526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6402-L6526)

## Overview
A PostgreSQL built-in function that converts a numeric value to a formatted text string using a specified format pattern.

## Definition

```c
Datum
numeric_to_char(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements PostgreSQL's TO_CHAR functionality for converting numeric values into formatted text strings. It takes a Numeric value and a format pattern, then produces a formatted string representation according to the pattern specifications.

The function handles several special formatting modes:
- Roman numeral conversion (RN/rn): Rounds the numeric value to an integer and converts it to Roman numerals
- Scientific notation (EEEE): Uses numeric_out_sci to format with exponential notation, handling special cases like NaN, Infinity, and -Infinity
- Standard formatting: Applies multiplicative scaling if specified, rounds according to format precision, and handles overflow conditions

The function manages sign handling, decimal alignment, padding with spaces or '#' characters for overflow, and proper formatting of special numeric values. It integrates with PostgreSQL's comprehensive formatting system using NUM_TOCHAR_prepare and NUM_TOCHAR_finish macros.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro, which provides access to:
  - : Input numeric value to format
  - : Format pattern string specifying desired output format

## Dependencies
- Functions called/Symbols referenced:
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish, numeric_round, numeric_out_sci
  - [int_to_roman](../i/int_to_roman.md), numeric_int4, numeric_out, numeric_power, numeric_mul
  - [int64_to_numeric](../i/int64_to_numeric.md), fill_str, DirectFunctionCall1, DirectFunctionCall2
  - [DatumGetNumeric](../D/DatumGetNumeric.md), NumericGetDatum, DatumGetInt32, DatumGetCString
  - IS_ROMAN, IS_EEEE, IS_MULTI (formatting condition macros)
- Called from (representative examples):
  - This is a SQL-callable function, typically not called directly from C code

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as TO_CHAR(numeric, text)
- Handles special numeric values (NaN, Infinity, -Infinity) with appropriate formatting
- Roman numeral conversion supports both uppercase (RN) and lowercase (rn) variants
- Scientific notation includes proper sign alignment for positive numbers
- Overflow conditions are handled by filling the output with '#' characters
- Supports multiplicative formatting patterns (V) that scale the input value
- Memory management includes proper allocation for output buffers of varying sizes
- Integrates with PostgreSQL's locale-aware number formatting system
- Returns a PostgreSQL text datum that can be used in SQL contexts
- Part of PostgreSQL's comprehensive formatting system in formatting.c