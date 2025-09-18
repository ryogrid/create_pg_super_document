# pg_strtoint16_safe

## Location
src/backend/utils/adt/numutils.c: 127 - 382

## Overview
Safely converts a string representation of a number to a signed 16-bit integer with comprehensive error handling and support for multiple number bases.

## Definition
```c
int16 pg_strtoint16_safe(const char *s, Node *escontext)
```

## Detailed Description
This function provides robust string-to-integer conversion for 16-bit signed integers with advanced error handling capabilities. It implements a two-path parsing strategy: a fast path optimized for common base-10 numbers without separators, and a comprehensive slow path that handles all supported formats including hexadecimal (0x/0X), octal (0o/0O), binary (0b/0B), and underscore digit separators.

The function uses unsigned arithmetic internally to properly handle two's complement representation, particularly for the most negative 16-bit value. It supports flexible input formatting including leading/trailing whitespace, optional sign characters, and underscore separators between digits for improved readability.

Error handling is performed through PostgreSQL's `ereturn()` mechanism, which allows errors to be either thrown immediately or captured in an ErrorSaveContext for later processing, depending on the `escontext` parameter.

## Parameters / Member Variables
- `s`: A null-terminated string containing the number to convert, supporting:
  - Leading/trailing whitespace
  - Optional '+' or '-' sign
  - Base prefixes (0x/0X for hex, 0o/0O for octal, 0b/0B for binary)
  - Underscore separators between digits
- `escontext`: Error context node for handling conversion errors; if NULL, errors are thrown via `ereport()`

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction hint)
  - PG_INT16_MIN (minimum 16-bit signed integer constant)
  - PG_INT16_MAX (maximum 16-bit signed integer constant)
  - ereturn (error return mechanism)
- Called from (representative examples):
  - [int2in](../i/int2in.md)
  - [pg_strtoint16](pg_strtoint16.md)

## Notes and Other Information
- Implements a performance-optimized two-path parsing strategy: fast path for simple decimal numbers, slow path for complex formats
- Uses unsigned arithmetic accumulation to handle two's complement edge cases correctly
- Supports hexadecimal digits via `hexlookup` table for efficient conversion
- Validates underscore placement rules: not at the beginning/end, must be between valid digits
- Returns proper PostgreSQL error codes: ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE for overflow, ERRCODE_INVALID_TEXT_REPRESENTATION for syntax errors
- The function name follows PostgreSQL's `_safe` convention for functions that use ErrorSaveContext
- Handles the full range of 16-bit signed integers (-32,768 to 32,767)
- Branch prediction hints (`likely`/`unlikely`) optimize performance for common cases