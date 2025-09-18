# pg_strtoint16

## Location
src/backend/utils/adt/numutils.c: 121 - 126

## Overview
Converts a string representation of a number to a signed 16-bit integer, supporting multiple number bases and formatting options.

## Definition
```c
int16 pg_strtoint16(const char *s)
```

## Detailed Description
This function provides a convenient wrapper around `pg_strtoint16_safe()` for converting string input to a 16-bit signed integer. It supports multiple number formats including decimal, hexadecimal (0x/0X prefix), octal (0o/0O prefix), and binary (0b/0B prefix) representations. The function handles signed numbers with optional '+' or '-' prefixes and allows flexible formatting with leading/trailing whitespace and optional underscore digit separators for improved readability.

The function uses two's complement representation internally by accumulating the input as an unsigned number, which properly handles the most negative 16-bit value that cannot be represented as a positive signed number.

Unlike its safe counterpart, this function will throw an `ereport()` on invalid input or overflow conditions, making it suitable for contexts where error handling should be performed via PostgreSQL's standard error reporting mechanism.

## Parameters / Member Variables
- `s`: A null-terminated string containing the number to be converted, which may include:
  - Optional leading/trailing whitespace
  - Optional sign character ('+' or '-')
  - Base prefixes (0x/0X for hex, 0o/0O for octal, 0b/0B for binary)
  - Digits optionally separated by underscores for readability

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strtoint16_safe](pg_strtoint16_safe.md)
- Called from (representative examples):
  - (No direct references found in the codebase)

## Notes and Other Information
- This is a thin wrapper that calls `pg_strtoint16_safe(s, NULL)` 
- Throws `ereport()` errors on invalid input format or numeric overflow
- Supports the full range of 16-bit signed integers (-32,768 to 32,767)
- Underscore separators between digits are allowed for readability but do not affect the numeric value
- The function handles two's complement representation properly for edge cases
- For error-safe parsing where exceptions should be avoided, use `pg_strtoint16_safe()` instead
- Part of PostgreSQL's numeric utility functions for robust string-to-integer conversion