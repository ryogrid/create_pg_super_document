# pg_ultostr

## Location
src/backend/utils/adt/numutils.c: 1309 - 1314

## Overview
Converts an unsigned 32-bit integer to its decimal string representation without NUL termination, designed for efficient building of multi-component strings.

## Definition
char *pg_ultostr(char *str, uint32 value)

## Detailed Description
This function provides a simple interface for converting a uint32 value into its decimal string representation at the specified memory location. It serves as a streamlined wrapper around pg_ultoa_n, designed specifically for scenarios where multiple numeric values need to be concatenated into a single string without intermediate NUL terminators. The function is optimized for string building patterns and returns a pointer to the position immediately following the last written character, enabling efficient chaining of multiple conversions.

## Parameters / Member Variables
- str: Pointer to the destination buffer where the string representation will be written (caller must ensure sufficient space)
- value: The unsigned 32-bit integer value to convert to decimal string representation

## Dependencies
- Functions called/Symbols referenced:
  - pg_ultoa_n (performs the actual integer-to-string conversion)
- Called from (representative examples):
  - AppendSeconds (datetime.c:455, 495)

## Notes and Other Information
- Returns a pointer to the position immediately after the last written character (not NUL-terminated)
- Simpler alternative to pg_ultostr_zeropad when zero-padding is not required
- Primarily used in datetime formatting for appending numeric components
- Caller must ensure destination buffer has adequate space (at least 10 bytes for uint32 values)
- Part of PostgreSQL's optimized number-to-string conversion utilities
- Designed for performance in string concatenation scenarios where multiple numeric values are combined