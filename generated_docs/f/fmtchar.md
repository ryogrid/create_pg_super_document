# fmtchar

## Location
[src/port/snprintf.c:1118-1135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1118-L1135)

## Overview
Formats and outputs a single character value with specified padding and alignment for the %c format specifier.

## Definition

```c
static void
fmtchar(int value, int leftjust, int minlen, PrintfTarget *target)
```
## Detailed Description
This function handles the formatting and output of single character values (%c format specifier) in PostgreSQL's portable snprintf implementation. It applies width formatting and padding to character arguments while respecting alignment specifications. Since a character always occupies exactly one position, the function focuses primarily on padding calculations and proper alignment handling.

The function calculates any necessary padding based on the minimum field width, outputs leading padding for right-justified formatting, outputs the single character, and finally outputs trailing padding for left-justified formatting.

## Parameters / Member Variables
- `value`: The integer value representing the character to be output (typically from char promotion)
- `leftjust`: Flag indicating left justification (1 for left-justified, 0 for right-justified)
- `minlen`: Minimum field width; if 1 (the character width) is less than this, padding will be added
- `*target`: Output destination structure containing formatting state and output buffer
## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (struct type for output destination)
  - [compute_padlen](../c/compute_padlen.md) (helper function to calculate padding requirements)
  - [dopr_outchmulti](../d/dopr_outchmulti.md) (function to output multiple identical characters for padding)
  - [dopr_outch](../d/dopr_outch.md) (function to output a single character to target)
  - [trailing_pad](../t/trailing_pad.md) (function to output trailing padding)

- Called from (representative examples):
  - [dopr](../d/dopr.md) (main printf formatting function)
  - [flushbuffer](flushbuffer.md) (output buffer management function)

## Notes and Other Information
- Part of PostgreSQL's platform-independent printf implementation
- Handles both left-justified (%-c) and right-justified (%c) character formatting
- Always treats the character as occupying exactly one position for padding calculations
- Padding is performed with space characters only
- The character value is passed as int due to C's argument promotion rules for char types
- Does not support precision specifiers since they are not meaningful for single characters
- Simpler than other formatting functions since characters have fixed width and no sign considerations

## Simplified Source

```c
static void
fmtchar(int value, int leftjust, int minlen, PrintfTarget *target)
{
    int padlen;

    // Calculate padding needed for minimum field width
    padlen = compute_padlen(minlen, 1, leftjust);

    // Output leading padding for right-justified
    if (padlen > 0) {
        dopr_outchmulti(' ', padlen, target);
        padlen = 0;
    }

    // Output the character
    dopr_outch(value, target);

    // Output trailing padding for left-justified
    trailing_pad(padlen, target);
}
```