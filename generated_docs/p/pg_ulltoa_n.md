# pg_ulltoa_n

## Location
[src/backend/utils/adt/numutils.c:1142-1228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L1142-L1228)

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

## Simplified Source

```c
// Simplified version of pg_ulltoa_n
int pg_ulltoa_n(uint64 value, char *a) {
    int olength, i = 0;
    uint32 value2;

    // Handle zero case
    if (value == 0) {
        *a = '0';
        return 1;
    }

    olength = decimalLength64(value);

    // Process large values (>= 100,000,000) in 8-digit chunks
    while (value >= 100000000) {
        uint64 q = value / 100000000;
        uint32 remainder = (uint32)(value - 100000000 * q);

        // Convert 8-digit chunk using optimized digit table lookups
        char *pos = a + olength - i;
        // ... convert remainder to 8 digits at pos-8 through pos-1 ...

        value = q;
        i += 8;
    }

    // Switch to 32-bit processing for remaining digits
    value2 = (uint32)value;

    // Process remaining digits in 4-digit, 2-digit, 1-digit chunks
    // using same optimization as pg_ultoa_n

    return olength;
}
```

Key simplifications made:
- Preserved core 64-bit to 32-bit conversion strategy
- Simplified the chunked processing logic
- Maintained performance-critical structure
- Focused on the essential algorithm flow
- Abstracted detailed digit table operations