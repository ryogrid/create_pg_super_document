# pg_strtoint32

## Location
[src/backend/utils/adt/numutils.c:383-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L383-L388)

## Overview
Converts a string representation of a number to a signed 32-bit integer, supporting multiple number bases and formatting options.

## Definition
```c
int32 pg_strtoint32(const char *s)
```

## Detailed Description
This function provides a convenient wrapper around `pg_strtoint32_safe()` for converting string input to a 32-bit signed integer. It supports multiple number formats including decimal, hexadecimal (0x/0X prefix), octal (0o/0O prefix), and binary (0b/0B prefix) representations. The function handles signed numbers with optional '+' or '-' prefixes and allows flexible formatting with leading/trailing whitespace and optional underscore digit separators for improved readability.

The function uses two's complement representation internally by accumulating the input as an unsigned number, which properly handles the most negative 32-bit value that cannot be represented as a positive signed number.

Unlike its safe counterpart, this function will throw an `ereport()` on invalid input or overflow conditions, making it suitable for contexts where error handling should be performed via PostgreSQL's standard error reporting mechanism.

## Parameters / Member Variables
- `s`: A null-terminated string containing the number to be converted, which may include:

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strtoint32_safe](pg_strtoint32_safe.md)
- Called from (representative examples):
  - [pq_parse_errornotice](pq_parse_errornotice.md)
  - [libpqrcv_identify_system](../l/libpqrcv_identify_system.md)
  - [libpqrcv_endstreaming](../l/libpqrcv_endstreaming.md)
  - [prsd_headline](prsd_headline.md)
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md)
  - [text_format](../t/text_format.md)

## Notes and Other Information
- This is a thin wrapper that calls `pg_strtoint32_safe(s, NULL)`
- Throws `ereport()` errors on invalid input format or numeric overflow
- Supports the full range of 32-bit signed integers (-2,147,483,648 to 2,147,483,647)
- Underscore separators between digits are allowed for readability but do not affect the numeric value
- The function handles two's complement representation properly for edge cases
- For error-safe parsing where exceptions should be avoided, use `pg_strtoint32_safe()` instead
- Widely used throughout PostgreSQL for parsing integer values in various contexts including libpq message processing, replication, text search, and array utilities
- Part of PostgreSQL's numeric utility functions for robust string-to-integer conversion