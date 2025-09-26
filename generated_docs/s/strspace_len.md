# strspace_len

## Location
src/backend/utils/adt/formatting.c: 2378 - 2399

## Overview
A utility function that counts the number of leading whitespace characters in a string.

## Definition

```c
static int
strspace_len(const char *str)
```
## Detailed Description
This static function scans through the beginning of a string and counts consecutive whitespace characters. It uses the  function to identify whitespace characters and advances through the string until it encounters a non-whitespace character or reaches the end of the string. The function is primarily used in PostgreSQL's formatting module to handle whitespace parsing during date/time and number formatting operations.

## Parameters / Member Variables
- : Input string to scan for leading whitespace characters

## Dependencies
- Functions called/Symbols referenced:
  - isspace() (standard C library function)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1055)
  - from_char_parse_int_len (formatting.c:2474)

## Notes and Other Information
- This is a static function within the formatting.c file, so it's only accessible within that compilation unit
- Returns the count of leading whitespace characters as an integer
- Uses  cast for  to handle potential negative char values safely
- Part of PostgreSQL's date/time and numeric formatting infrastructure