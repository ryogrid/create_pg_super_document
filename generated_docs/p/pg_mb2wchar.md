# pg_mb2wchar

## Location
src/backend/utils/mb/mbutils.c: 979 - 985

## Overview
Converts a null-terminated multibyte string from the database encoding to an array of wide characters (Unicode code points).

## Definition
```c
int pg_mb2wchar(const char *from, pg_wchar *to)
```

## Detailed Description
This function serves as a convenience wrapper that converts a null-terminated multibyte string to wide characters using the database's current encoding. It operates by:

1. **Automatic length calculation**: Uses `strlen()` to determine the input string length
2. **Encoding dispatch**: Delegates to the appropriate encoding-specific conversion function via `pg_wchar_table[DatabaseEncoding->encoding].mb2wchar_with_len`
3. **Database encoding context**: Always uses the current database encoding for conversion

The function is part of PostgreSQL's multi-byte character handling infrastructure and provides a simplified interface for string-to-wchar conversion when the input length doesn't need to be explicitly specified.

## Parameters / Member Variables
- `from`: Source null-terminated multibyte string in database encoding
- `to`: Destination buffer for wide characters (pg_wchar array), must be large enough to hold the converted result

## Dependencies
- Functions called/Symbols referenced:
  - pg_wchar_table (encoding function dispatch table)
  - DatabaseEncoding (global database encoding structure)
  - strlen (standard C library function)
  - mb2wchar_with_len (encoding-specific conversion function)
- Called from (representative examples):
  - No direct references found in current codebase (utility function)

## Notes and Other Information
- This is a thin wrapper around the length-aware conversion function `mb2wchar_with_len`
- The function assumes the input string is properly null-terminated
- Returns the number of wide characters produced (excluding any null terminator)
- The caller is responsible for ensuring the output buffer is sufficiently large
- Part of PostgreSQL's comprehensive multi-byte character encoding support system
- Uses the global `pg_wchar_table` dispatch mechanism for encoding-specific operations
- Located in src/backend/utils/mb/mbutils.c:979-985