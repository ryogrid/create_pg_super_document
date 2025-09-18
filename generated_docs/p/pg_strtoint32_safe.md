# pg_strtoint32_safe

## Location
src/backend/utils/adt/numutils.c: 389 - 644

## Overview
Converts a string representation of an integer to a signed 32-bit integer value with error handling support, supporting multiple number bases and underscore separators for readability.

## Definition


## Detailed Description
This function provides a robust string-to-integer conversion with comprehensive error handling. It implements a two-phase parsing strategy: a fast path for simple base-10 numbers and a slower comprehensive path that handles hexadecimal (0x/0X), octal (0o/0O), binary (0b/0B), and decimal formats. The function supports underscore separators between digits for improved readability and handles both positive and negative numbers with proper overflow detection.

The function uses unsigned arithmetic internally to correctly handle the full range of 32-bit signed integers, including the most negative value that cannot be represented as a positive number in two's complement representation.

## Parameters / Member Variables
- : Input string containing the integer representation to convert
- : Error context node for soft error handling; if NULL, errors are thrown via ereport()

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction macro)
  - PG_INT32_MIN (minimum 32-bit signed integer constant)
  - PG_INT32_MAX (maximum 32-bit signed integer constant)
  - ereturn (error return macro for soft error handling)
- Called from (representative examples):
  - int4in (integer input function)
  - pg_strtoint32 (wrapper function without error context)

## Notes and Other Information
- Uses a fast path optimization for common base-10 integers without underscores
- Supports multiple number bases: decimal, hexadecimal (0x), octal (0o), and binary (0b)
- Allows underscore separators between digits for readability (e.g., 1_000_000)
- Handles leading and trailing whitespace
- Provides soft error handling through ErrorSaveContext when escontext is provided
- Uses unsigned arithmetic internally to handle two's complement edge cases correctly
- Part of PostgreSQL's robust numeric input parsing infrastructure