# int_to_roman

## Location
[src/backend/utils/adt/formatting.c:5238-5286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5238-L5286)

## Overview
Converts an integer number to its Roman numeral representation as a string.

## Definition


## Detailed Description
This function converts a positive integer (1-3999) into its Roman numeral equivalent. The function uses predefined arrays , , and  containing Roman numeral patterns for units, tens, and hundreds respectively. Numbers above 3999 or below 1 are considered invalid and result in a string of '#' characters.

The algorithm works by:
1. Converting the number to a string representation to process each digit
2. Processing digits from left to right based on their positional value
3. For thousands (len > 3), appending 'M' characters repeatedly
4. For hundreds, tens, and units, using the appropriate Roman numeral pattern from the lookup arrays

## Parameters / Member Variables
- : The integer to convert to Roman numerals (valid range: 1-3999)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [fill_str](../f/fill_str.md) (fills result with '#' for invalid numbers)
  - snprintf (string formatting)
  - strcat (string concatenation)
  - rm1[] (Roman numeral patterns for units 1-9)
  - rm10[] (Roman numeral patterns for tens 10-90)
  - rm100[] (Roman numeral patterns for hundreds 100-900)
- Called from (representative examples):
  - [numeric_to_char](../n/numeric_to_char.md) (formatting.c:6428)
  - [int4_to_char](int4_to_char.md) (formatting.c:6546)
  - [int8_to_char](int8_to_char.md) (formatting.c:6642)
  - [float4_to_char](../f/float4_to_char.md) (formatting.c:6743)
  - [float8_to_char](../f/float8_to_char.md) (formatting.c:6845)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Invalid numbers (< 1 or > 3999) return a string of 15 '#' characters
- Roman numerals follow standard conventions with subtractive notation (IV, IX, XL, XC, CD, CM)
- Used primarily in PostgreSQL's formatting system for converting numbers to Roman numerals in to_char() functions
- Maximum supported value is 3999 (MMMCMXCIX) due to Roman numeral system limitations