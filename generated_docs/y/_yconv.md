# _yconv

## Location
[src/timezone/strftime.c:541-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/strftime.c#L541-L546)

## Overview
A static helper function that handles year conversion for strftime formatting, specifically managing the %C (century) and %y (year within century) format specifiers with proper handling of negative years and years exceeding 9999.

## Definition

```c
static char *
_yconv(int a, int b, bool convert_top, bool convert_yy,
	   char *pt, const char *ptlim)
```
## Detailed Description
The  function implements year conversion logic for PostgreSQL's timezone strftime functionality. It addresses ambiguities in POSIX and C Standard specifications regarding how %C and %y format specifiers should behave with negative years or years exceeding 9999. The function follows the convention that %C concatenated with %y yields the same output as %Y, ensuring at least 4 bytes of output with additional bytes only when necessary.

The function performs arithmetic operations to split a year value into century (lead) and year-within-century (trail) components, handling the complex cases of negative years and ensuring proper sign handling across the boundary between negative and positive values.

## Parameters / Member Variables
- : First integer component for year calculation (typically the base year value)
- : Second integer component for year calculation (typically an offset or adjustment)
- : Boolean flag indicating whether to convert and output the century part (%C)
- : Boolean flag indicating whether to convert and output the year-within-century part (%y)
- : Pointer to the current position in the output buffer where formatted output should be written
- : Pointer to the limit of the output buffer to prevent buffer overflows

## Dependencies
- Functions called/Symbols referenced:
  - : Helper function for adding string literals to the output buffer
  - : Helper function for converting integers to formatted strings
- Called from (representative examples):
  - : Main strftime formatting function (multiple call sites at lines 196, 401, 406, 445, 450)

## Notes and Other Information
- Uses a DIVISOR constant of 100 to separate century from year-within-century
- Implements complex sign handling logic to ensure proper formatting of negative years
- Special case handling for lead=0 and trail<0, outputting "-0" for the century part
- The function maintains consistency with POSIX strftime behavior while extending support for edge cases
- Part of PostgreSQL's timezone handling subsystem, located in src/timezone/strftime.c:541-571