# pg_ulltoa_n

## Location
src/backend/utils/adt/numutils.c: 1142 - 1228

## Overview
Converts an unsigned 64-bit integer to its string representation without null termination and returns the length of the resulting string.

## Definition
```c
int pg_ulltoa_n(uint64 value, char *a)
```

## Detailed Description
The `pg_ulltoa_n` function is a high-performance PostgreSQL utility function that converts an unsigned 64-bit integer (`uint64`) to its decimal string representation. Like `pg_ultoa_n`, this function does NOT null-terminate the output string, making it suitable for situations where the string will be further processed or embedded within larger strings.

The function implements a sophisticated multi-stage optimization algorithm specifically designed for 64-bit integers:

1. **First stage**: Processes values >= 100,000,000 by extracting 8-digit chunks, handling them as 32-bit values, and processing 4 digits at a time within each chunk
2. **Second stage**: Switches to 32-bit arithmetic for remaining digits (value < 100,000,000) for better performance  
3. **Final stages**: Uses the same optimized 4-digit, 2-digit, and single-digit processing as `pg_ultoa_n`

This hybrid approach leverages the efficiency of 32-bit operations while correctly handling the full 64-bit range up to 18,446,744,073,709,551,615.

## Parameters / Member Variables
- `value`: The unsigned 64-bit integer to convert to string representation
- `a`: Pointer to the output buffer where the string representation will be written (must have at least MAXINT8LEN bytes available, typically 20 bytes for worst case)

## Dependencies
- Functions called/Symbols referenced:
  - [decimalLength64](../d/decimalLength64.md) (calculates the number of digits needed for 64-bit values)
  - DIGIT_TABLE (lookup table for digit pairs, same as used by 32-bit functions)
- Called from (representative examples):
  - [BuildQueryCompletionString](../B/BuildQueryCompletionString.md) (for formatting query completion statistics)
  - [pg_lltoa](pg_lltoa.md) (as part of signed 64-bit integer conversion)

## Notes and Other Information
- The output string is NOT null-terminated - caller must handle termination if needed
- Requires at least MAXINT8LEN bytes of buffer space (typically 20 bytes for maximum uint64 value)
- Optimized with hybrid 64-bit/32-bit processing for maximum performance
- Processes up to 8 digits at once in the first stage for very large numbers
- Switches to 32-bit arithmetic once value fits in 32 bits for better performance
- Returns string length, eliminating need for separate strlen() call  
- Used as building block for 64-bit signed integer conversion
- Critical for PostgreSQL's bigint data type and internal counters that use 64-bit values
- Algorithm processes digits right-to-left like its 32-bit counterpart