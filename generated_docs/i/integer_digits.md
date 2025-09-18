# integer_digits

## Location
[src/fe_utils/print.c:278-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L278-L288)

## Overview
Counts the number of digits in the integral part of a numeric string, used for formatting numeric values with locale-specific separators.

## Definition


## Detailed Description
The  function is a utility function that counts the number of consecutive digits at the beginning of a numeric string, effectively determining the length of the integral (non-fractional) part of a number. The function ignores any leading sign character ('+' or '-') and uses  to count consecutive characters that are decimal digits (0-9). This information is typically used for formatting numbers with thousands separators or other locale-specific numeric formatting.

## Parameters / Member Variables
- : A null-terminated string representing a numeric value that may include a leading sign character

## Dependencies
- Functions called/Symbols referenced:
  - strspn (standard C library function)
- Called from (representative examples):
  - [additional_numeric_locale_len](../a/additional_numeric_locale_len.md)
  - [format_numeric_locale](../f/format_numeric_locale.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (src/fe_utils/print.c)
- The function handles both positive and negative numbers by skipping over the sign character
- Uses strspn() for efficient counting of consecutive digit characters
- Returns the count as an integer value
- Does not validate that the input string is a properly formatted number