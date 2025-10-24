# count_spaces_until

## Location
[src/tools/pg_bsd_indent/io.c:517-549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L517-L549)

## Overview
Calculates where the character position will be after printing text from a buffer starting at a given column position, with support for bounded string processing.

## Definition

```c
int
count_spaces_until(int cur, char *buffer, char *end)
/*
 * this routine figures out where the character position will be after
 * printing the text in buffer starting at column "current"
 */
```
## Detailed Description
The  function is a utility in the PostgreSQL BSD indent tool that determines the final column position that would result from printing a specific portion of text starting from a given current position. This function is similar to  but includes boundary checking by accepting an  pointer, allowing it to process only a substring of the buffer.

The function processes each character in the buffer and updates the column position according to the character type:
- Newlines and form feeds reset position to column 1
- Tabs advance to the next tab stop based on   
- Backspace characters move backward one position
- All other printable characters advance one position

This bounded version is particularly useful when analyzing comment text or partial strings where only a portion of the buffer needs to be considered for positioning calculations.

## Parameters / Member Variables
- `cur`: The current column position to start calculation from (integer)
- `*buffer`: Pointer to the character buffer to analyze
- `*end`: Pointer marking the end boundary of the text to process
## Dependencies
- Functions called/Symbols referenced:
  - Uses  global variable for tab stop calculations
- Called from (representative examples):
  - : Used in main indentation logic for positioning calculations
  - : Called by the simpler count_spaces wrapper function
  - : Used in comment processing for alignment calculations

## Notes and Other Information
- Originally coded in November 1976 by D A Willcox of CAC
- Returns the final column position after processing the specified text range
- Handles special control characters: newline ('\n'), form feed (014 octal), tab ('\t'), and backspace (010 octal)
- The  parameter allows processing bounded strings without requiring null-termination
- Processing stops when encountering null terminator or reaching the  pointer, whichever comes first
- Essential for precise positioning calculations in comment formatting and code alignment
- More flexible than  due to its ability to process partial strings

## Simplified Source

```c
int count_spaces_until(int cur, char *buffer, char *end) {
    char *buf;

    // Process each character until null terminator or end boundary
    for (buf = buffer; *buf != '\0' && buf != end; ++buf) {
        switch (*buf) {
        case '\n':
        case 014:  // form feed
            cur = 1;
            break;

        case '\t':
            // Advance to next tab stop
            cur = tabsize * (1 + (cur - 1) / tabsize) + 1;
            break;

        case 010:  // backspace
            --cur;
            break;

        default:
            // Regular character
            ++cur;
            break;
        }
    }

    return cur;
}
```