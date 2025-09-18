# pg_strtoint64

## Location
src/backend/utils/adt/numutils.c: 645 - 650

## Overview
A wrapper function that converts a string representation to a signed 64-bit integer by calling the safe version with NULL error context.

## Definition


## Detailed Description
This function serves as a simple wrapper around pg_strtoint64_safe, providing the traditional interface where parsing errors result in thrown exceptions rather than soft error handling. It supports all the same features as the safe version including multiple number bases (decimal, hexadecimal with 0x/0X prefix, octal with 0o/0O prefix, binary with 0b/0B prefix), underscore digit separators for readability, and proper handling of leading/trailing whitespace.

The function uses unsigned arithmetic internally to correctly handle two's complement representation, particularly for the most negative 64-bit value that cannot be represented as a positive signed number.

## Parameters / Member Variables
- : Input string containing the integer representation to convert

## Dependencies
- Functions called/Symbols referenced:
  - pg_strtoint64_safe (the underlying implementation function)
- Called from (representative examples):
  - Currently no direct callers found in the codebase

## Notes and Other Information
- This is a convenience wrapper that throws errors via ereport() instead of using soft error handling
- Supports the same comprehensive parsing features as pg_strtoint64_safe
- Part of PostgreSQL's numeric input parsing infrastructure
- The comprehensive format support includes underscore separators (e.g., 1_000_000) and multiple bases
- Uses two's complement arithmetic handling for full 64-bit signed integer range