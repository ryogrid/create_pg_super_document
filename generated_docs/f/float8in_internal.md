# float8in_internal

## Location
[src/backend/utils/adt/float.c:388-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L388-L514)

## Overview
Internal implementation function for converting string input to float8 (double precision) values, providing platform-independent parsing with advanced error handling and support for special floating-point values.

## Definition


## Detailed Description
This function serves as the core implementation for parsing string representations of double-precision floating-point numbers. It extends standard C library  functionality with PostgreSQL-specific error handling, whitespace management, and support for special values like NaN and Infinity. The function is designed to be reusable across different PostgreSQL data types that need to parse floating-point substrings.

Key features include:
- Leading and trailing whitespace handling
- Platform-independent parsing of special values (NaN, Infinity, +/-Inf)
- Comprehensive error reporting with context information
- Support for soft error handling through ErrorSaveContext
- Range validation for denormalized numbers

## Parameters / Member Variables
- : Input string containing the number to parse (modifiable for whitespace skipping)
- : Optional pointer to receive the position where parsing stopped (NULL means require complete consumption)
- : Name of the calling data type for error messages (e.g., "double precision", "point")
- : Original input string for error reporting (may be larger than the parsed substring)
- : Error context for soft error handling (NULL for normal error throwing)

## Dependencies
- Functions called/Symbols referenced:
  - ereturn (error handling with context support)
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (case-insensitive string comparison)
  - get_float8_nan (retrieve NaN value)
  - get_float8_infinity (retrieve positive infinity value)
  - strtod (standard C library function)
  - isspace (standard C library function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)

- Called from (representative examples):
  - [float8in](float8in.md) (main float8 input function)
  - single_decode (geometric operations)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (JSON path execution)

## Notes and Other Information
- Handles platform differences in strtod() behavior for special values
- Provides more robust error messages than standard strtod()
- Supports both strict parsing (when endptr_p is NULL) and partial parsing
- Special handling for denormalized numbers that might incorrectly trigger ERANGE
- Designed for reuse in composite types like point, box, etc. where floating-point values are parsed as substrings