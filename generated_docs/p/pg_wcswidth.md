# pg_wcswidth

## Location
src/fe_utils/mbprint.c: 177 - 210

## Overview
A public function that calculates the display width of a multibyte character string, assuming all characters will appear on a single line.

## Definition
```c
int pg_wcswidth(const char *pwcs, size_t len, int encoding)
```

## Detailed Description
This function computes the total display width of a multibyte string by iterating through each character and summing their individual display widths. It serves as a "dumb" display-width function that makes the simplifying assumption that all characters will appear on one line, making it easier to use than `pg_wcssize` when this assumption holds.

The function processes the string character by character:
1. Determines the byte length of each character using `PQmblen`
2. Validates that the remaining string length is sufficient for the complete character
3. Gets the display width of the character using `PQdsplen`
4. Accumulates positive display widths (ignoring zero-width characters)
5. Advances to the next character and repeats

## Parameters / Member Variables
- `pwcs`: Pointer to the multibyte character string to measure
- `len`: Length of the string in bytes
- `encoding`: Character encoding identifier for the string

## Dependencies
- Functions called/Symbols referenced:
  - [PQmblen](../P/PQmblen.md)
  - [PQdsplen](../P/PQdsplen.md)
- Called from (representative examples):
  - [describeOneTableDetails](../d/describeOneTableDetails.md)
  - [lineptr](../l/lineptr.md) (via header include)

## Notes and Other Information
- This is a public function (not static) intended for external use
- Designed for simple use cases where text wrapping is not a concern
- Gracefully handles invalid strings by breaking early when insufficient bytes remain
- Only counts characters with positive display width (zero-width characters are ignored)
- Used primarily in PostgreSQL's frontend utilities for table formatting and display
- More efficient than `pg_wcssize` when line wrapping calculations are not needed
- Essential for proper alignment and formatting of multibyte text in terminal output