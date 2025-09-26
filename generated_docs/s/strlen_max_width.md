# strlen_max_width

## Location
src/fe_utils/print.c: 3746 - 3776

## Overview
Computes the byte distance to the end of a string or a target display width limit, whichever comes first, while accounting for multibyte character encodings.

## Definition
```c
static int strlen_max_width(unsigned char *str, int *target_width, int encoding)
```

## Detailed Description
This static function calculates how many bytes from the beginning of a string are needed to reach either the end of the string or a specified target display width, measured in display character positions. The function is encoding-aware and properly handles multibyte characters by using PostgreSQL's display length and multibyte length functions.

The function iterates through the string character by character, accumulating display width until either the string ends or the target width would be exceeded. It ensures that at least the first character is always included, even if that character alone exceeds the target width. The target_width parameter is updated to reflect the actual display width consumed.

This function is crucial for table formatting operations where text needs to be truncated or wrapped at specific display width boundaries while respecting character encoding boundaries.

## Parameters / Member Variables
- `str`: Pointer to the input string to measure (as unsigned char*)
- `target_width`: Pointer to integer specifying maximum display width; updated with actual width consumed
- `encoding`: Character encoding identifier used for proper multibyte character handling

## Dependencies
- Functions called/Symbols referenced:
  - PQdsplen (calculates display width of a character)
  - PQmblen (calculates byte length of a multibyte character)
- Called from (representative examples):
  - print_aligned_text (src/fe_utils/print.c:1086)
  - print_aligned_vertical (src/fe_utils/print.c:1662, 1736)

## Notes and Other Information
- Function is declared static, making it internal to the print.c compilation unit
- Handles invalid/corrupted strings by preventing buffer overruns
- Always includes at least the first character, even if it exceeds target width
- Essential for PostgreSQL's table formatting system in frontend utilities
- Properly accounts for the difference between byte length and display width in multibyte encodings
- Returns byte offset from string start, while updating target_width with actual display positions used