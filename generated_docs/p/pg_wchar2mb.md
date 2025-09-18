# pg_wchar2mb

## Location
src/backend/utils/mb/mbutils.c: 1001 - 1007

## Overview
Converts a wide character string to a multibyte character string using the current database encoding.

## Definition
```c
int pg_wchar2mb(const pg_wchar *from, char *to)
```

## Detailed Description
This function performs the reverse operation of pg_mb2wchar, converting a null-terminated wide character string (pg_wchar array) back to a multibyte character string. It automatically determines the length of the source wide character string using pg_wchar_strlen and delegates the actual conversion to the encoding-specific wchar2mb_with_len function stored in the pg_wchar_table array for the current database encoding.

## Parameters / Member Variables
- `from`: Pointer to the source null-terminated wide character string to be converted
- `to`: Pointer to the destination buffer where the multibyte character string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - pg_wchar_table (global encoding table)
  - DatabaseEncoding (current database encoding setting)
  - wchar2mb_with_len (encoding-specific conversion function pointer)
  - [pg_wchar_strlen](pg_wchar_strlen.md) (calculates length of wide character string)
- Called from (representative examples):
  - No direct references found in the current codebase

## Notes and Other Information
- This function assumes the input wide character string is null-terminated
- The destination buffer must be large enough to hold the converted multibyte string
- Returns the number of bytes written to the destination buffer
- The function provides a convenient wrapper for full string conversion without requiring explicit length calculation
- Less commonly used compared to the length-limited variants, possibly due to buffer safety considerations