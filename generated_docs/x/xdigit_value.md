# xdigit_value

## Location
src/backend/utils/adt/numeric.c: 7231 - 7257

## Overview
A static inline utility function that converts a single hexadecimal digit character to its numeric value.

## Definition
```c
static inline int xdigit_value(char dig)
```

## Detailed Description
The `xdigit_value` function is a simple character-to-digit conversion utility that translates hexadecimal digit characters ('0'-'9', 'a'-'f', 'A'-'F') into their corresponding numeric values (0-15). The function handles both lowercase and uppercase hexadecimal letters, returning -1 for any character that is not a valid hexadecimal digit. This function is marked as `inline` for performance optimization since it's a simple, frequently-used utility function.

## Parameters / Member Variables
- `dig`: The character to convert to its hexadecimal digit value

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic character arithmetic)
- Called from (representative examples):
  - set_var_from_non_decimal_integer_str

## Notes and Other Information
This is a pure utility function with no side effects. It uses character arithmetic to efficiently convert digits by subtracting the character code of '0' for decimal digits, and adjusting by adding 10 for alphabetic hex digits. The function returns -1 for invalid characters, which allows callers to easily detect and handle invalid hexadecimal input. The inline designation suggests this function is expected to be called frequently enough that inlining provides a measurable performance benefit.