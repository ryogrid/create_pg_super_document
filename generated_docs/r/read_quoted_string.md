# read_quoted_string

## Location
src/bin/pg_dump/filter.c: 218 - 302

## Overview
Reads a quoted string that can span multiple lines and handles escape sequences, returning a pointer to the character after the closing quote.

## Definition
```c
static const char *read_quoted_string(FilterStateData *fstate, const char *str, PQExpBuffer pattern)
```

## Detailed Description
This function reads a quoted string from filter input that can span across multiple lines. It handles various escape sequences including double quotes ("") for literal quotes and backslash sequences (\n for newline, \\ for literal backslash). The function appends the parsed content to a PQExpBuffer and automatically reads additional lines from the file when needed. It properly handles line endings and maintains line number tracking for error reporting.

## Parameters / Member Variables
- `fstate`: Pointer to FilterStateData containing file handle, line buffer, and state information
- `str`: Pointer to the current position in the input string (should point to opening quote)
- `pattern`: PQExpBuffer to append the parsed quoted string content to

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBufferChar
  - pg_get_line_buf
  - [pg_log_filter_error](../p/pg_log_filter_error.md)
  - [exit_nicely](../e/exit_nicely.md) (via fstate function pointer)
  - FilterStateData (struct type)
- Called from (representative examples):
  - [read_pattern](read_pattern.md) (at src/bin/pg_dump/filter.c:342)

## Notes and Other Information
- This is a static function, only accessible within the filter.c file
- Handles multi-line quoted strings by automatically reading additional lines from the input file
- Supports escape sequences: \"\" for literal double quotes, \\n for newlines, \\\\ for literal backslashes
- Maintains line number tracking in the FilterStateData for accurate error reporting
- Will terminate the program via exit_nicely on file read errors or unexpected end of file
- Ignores trailing \r and \n characters as they are handled by pg_get_line_buf
- Part of pg_dump's filter file parsing mechanism