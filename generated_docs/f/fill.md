# fill

## Location
[src/interfaces/libpq/fe-print.c:755-762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-print.c#L755-L762)

## Overview
A simple utility function that outputs a specified number of filler characters to a file stream for padding and alignment purposes.

## Definition

```c
static void
fill(int length, int max, char filler, FILE *fp)
```
## Detailed Description
The  function is a straightforward utility that provides character padding functionality in PostgreSQL's libpq printing system. It calculates how many filler characters are needed to reach a target width and outputs them to the specified file stream. The function is designed to support text alignment and formatting by filling gaps with repeated characters.

The function works by:
1. Calculating the difference between the maximum desired width and current length
2. Outputting that many filler characters using  in a simple loop
3. Handling edge cases where length already equals or exceeds max

This is typically used for creating consistent column widths, padding fields to alignment boundaries, or generating decorative elements like borders or separators.

## Parameters / Member Variables
- : Current length of content (number of characters already used)
- : Maximum desired width (target column width or padding boundary)
- : The character to use for padding (e.g., space, dash, asterisk)
- : Output file stream where filler characters will be written

## Dependencies
- Functions called/Symbols referenced:
  - putc (standard C library function)
- Called from (representative examples):
  - DEFAULT_FIELD_SEP (src/interfaces/libpq/fe-print.c:631)
  - DEFAULT_FIELD_SEP (src/interfaces/libpq/fe-print.c:640)
  - DEFAULT_FIELD_SEP (src/interfaces/libpq/fe-print.c:653)

## Notes and Other Information
- This is a static utility function used internally within fe-print.c
- The function handles cases where length >= max by outputting zero characters
- Uses putc for character-by-character output rather than bulk string operations
- Simple and efficient implementation suitable for small padding operations
- No error checking or validation is performed on input parameters
- The loop condition  ensures proper handling of boundary conditions