# pg_wcssize

## Location
[src/fe_utils/mbprint.c:211-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/mbprint.c#L211-L293)

## Overview
Calculates the display dimensions and formatting size requirements for a multibyte string, providing essential metrics for text formatting and display in PostgreSQL frontend utilities.

## Definition

```c
void
pg_wcssize(const unsigned char *pwcs, size_t len, int encoding,
		   int *result_width, int *result_height, int *result_format_size)
```
## Detailed Description
pg_wcssize analyzes a multibyte character string and computes three key metrics needed for text display and formatting. It processes the string character by character, handling various control characters (newlines, tabs, carriage returns) and multibyte characters according to the specified encoding. The function is specifically designed to work in tandem with pg_wcsformat and must be kept synchronized with it.

The function handles special characters as follows:
- Newlines (): Increment height and reset line width
- Carriage returns (): Add 2 characters to width 
- Tabs (): Expand to next 8-character boundary
- Control characters: Display as escape sequences (\u0000 format for non-ASCII, 4 chars for ASCII)
- Regular characters: Add their display width

## Parameters / Member Variables
- `*pwcs`: Input multibyte character string to analyze
- `len`: Length of the input string in bytes
- `encoding`: Character encoding identifier for proper multibyte handling
- `*result_width`: Output parameter for the width in display characters of the longest line
- `*result_height`: Output parameter for the number of lines in the display output
- `*result_format_size`: Output parameter for the number of bytes required to store the formatted representation
## Dependencies
- Functions called/Symbols referenced:
  - [PQmblen](../P/PQmblen.md): Determines the byte length of a multibyte character
  - [PQdsplen](../P/PQdsplen.md): Determines the display width of a multibyte character
- Called from (representative examples):
  - [print_aligned_text](print_aligned_text.md): For calculating table formatting dimensions
  - [print_aligned_vertical](print_aligned_vertical.md): For vertical table formatting
  - [lineptr](../l/lineptr.md): Through header inclusion for line pointer operations

## Notes and Other Information
- This function MUST be kept in sync with pg_wcsformat to ensure consistent formatting behavior
- The function accounts for null terminators in format_size calculations
- Tab expansion follows standard 8-character tab stops
- Control characters are rendered as escape sequences, requiring additional space
- The function is located in src/fe_utils/mbprint.c and is part of PostgreSQL's frontend utilities for text display

## Simplified Source

```c
void pg_wcssize(const unsigned char *pwcs, size_t len, int encoding,
               int *result_width, int *result_height, int *result_format_size) {
    int linewidth = 0, width = 0, height = 1, format_size = 0;
    int chlen;

    for (; *pwcs && len > 0; pwcs += chlen) {
        chlen = PQmblen((const char *) pwcs, encoding);
        if (len < (size_t) chlen) break;

        int w = PQdsplen((const char *) pwcs, encoding);

        if (chlen == 1) {  // Single-byte character
            if (*pwcs == '\n') {
                // Newline: update max width, reset line, increment height
                if (linewidth > width) width = linewidth;
                linewidth = 0;
                height++;
                format_size++;
            } else if (*pwcs == '\r') {
                // Carriage return: display as visible chars
                linewidth += 2;
                format_size += 2;
            } else if (*pwcs == '\t') {
                // Tab: expand to 8-char boundary
                do {
                    linewidth++;
                    format_size++;
                } while (linewidth % 8 != 0);
            } else if (w < 0) {
                // Control char: display as escape sequence
                linewidth += 4;
                format_size += 4;
            } else {
                // Normal char: add display width
                linewidth += w;
                format_size++;
            }
        } else if (w < 0) {
            // Non-ASCII control char: \u0000 format
            linewidth += 6;
            format_size += 6;
        } else {
            // Regular multibyte char
            linewidth += w;
            format_size += chlen;
        }
        len -= chlen;
    }

    // Final line width check and null terminator
    if (linewidth > width) width = linewidth;
    format_size++;

    // Set output parameters
    if (result_width) *result_width = width;
    if (result_height) *result_height = height;
    if (result_format_size) *result_format_size = format_size;
}
```