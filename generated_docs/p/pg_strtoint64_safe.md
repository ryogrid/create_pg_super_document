# pg_strtoint64_safe

## Location
src/backend/utils/adt/numutils.c: 651 - 899

## Overview
Converts a string representation of an integer to a signed 64-bit integer value with comprehensive error handling, supporting multiple number bases and underscore separators.

## Definition


## Detailed Description
This function provides robust string-to-64-bit-integer conversion with comprehensive error handling and format support. Like its 32-bit counterpart, it implements a two-phase parsing strategy with a fast path for simple base-10 numbers and a comprehensive slow path for complex formats. It supports hexadecimal (0x/0X), octal (0o/0O), binary (0b/0B), and decimal number formats, along with underscore separators between digits for improved readability.

The function uses unsigned arithmetic internally to correctly handle the full range of 64-bit signed integers, including proper handling of the most negative value in two's complement representation. It provides both hard error (ereport) and soft error (ErrorSaveContext) handling modes.

## Parameters / Member Variables
- : Input string containing the integer representation to convert  
- : Error context node for soft error handling; if NULL, errors are thrown via ereport()

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction macro)
  - PG_INT64_MIN (minimum 64-bit signed integer constant)
  - PG_INT64_MAX (maximum 64-bit signed integer constant)  
  - ereturn (error return macro for soft error handling)
- Called from (representative examples):
  - make_const (parser node creation function)
  - int8in (bigint input function)
  - pg_strtoint64 (wrapper function without error context)

## Notes and Other Information
- Uses fast path optimization for common base-10 integers without special formatting
- Supports comprehensive number format parsing: decimal, hex (0x), octal (0o), binary (0b)
- Allows underscore digit separators for readability (e.g., 1_000_000_000)
- Handles leading and trailing whitespace gracefully
- Provides both hard and soft error handling through ErrorSaveContext
- Uses unsigned arithmetic internally for correct two's complement edge case handling
- Essential component of PostgreSQL's bigint type input processing
- Accumulates values as unsigned to handle the full signed range correctly