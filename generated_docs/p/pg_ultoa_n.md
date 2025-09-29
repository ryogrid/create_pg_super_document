# pg_ultoa_n

## Location
[src/backend/utils/adt/numutils.c:1057-1121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L1057-L1121)

## Overview
Converts an unsigned 32-bit integer to its string representation without null termination and returns the length of the resulting string.

## Definition
```c
int pg_ultoa_n(uint32 value, char *a)
```

## Detailed Description
The `pg_ultoa_n` function is a high-performance PostgreSQL utility function that converts an unsigned 32-bit integer (`uint32`) to its decimal string representation. Unlike standard library functions, this function does NOT null-terminate the output string, making it suitable for situations where the string will be further processed or where precise control over buffer contents is required.

The function implements an optimized conversion algorithm that processes digits in groups for better performance. It uses a pre-computed DIGIT_TABLE for efficient digit-to-character conversion and processes up to 4 digits at a time when possible. The algorithm works backwards from the end of the string, filling in digits from right to left.

Key optimization techniques:
- Processes 4 digits at once when value >= 10000
- Processes 2 digits at once when value >= 100
- Uses lookup table (DIGIT_TABLE) for digit conversion
- Pre-computes string length to avoid multiple passes

## Parameters / Member Variables
- `value`: The unsigned 32-bit integer to convert to string representation
- `a`: Pointer to the output buffer where the string representation will be written (must have at least 10 bytes available for worst case: 4,294,967,295)

## Dependencies
- Functions called/Symbols referenced:
  - [decimalLength32](../d/decimalLength32.md) (calculates the number of digits needed)
  - DIGIT_TABLE (lookup table for digit pairs)
- Called from (representative examples):
  - [printsimple](printsimple.md) (for debug output formatting)
  - [pg_ltoa](pg_ltoa.md) (as part of signed integer conversion)
  - [pg_ultostr_zeropad](pg_ultostr_zeropad.md) (for zero-padded string conversion)
  - [pg_ultostr](pg_ultostr.md) (for null-terminated string conversion)

## Notes and Other Information
- The output string is NOT null-terminated - caller must handle termination if needed
- Requires at least 10 bytes of buffer space (for maximum uint32 value 4,294,967,295)
- Optimized for performance with lookup tables and batch digit processing
- Returns string length, eliminating need for separate strlen() call
- Used as a building block for other string conversion functions in PostgreSQL
- The algorithm processes digits right-to-left for efficiency

## Simplified Source

```c
int pg_ultoa_n(uint32 value, char *a)
{
    int olength, i = 0;

    // Handle zero case
    if (value == 0) {
        *a = '0';
        return 1;
    }

    olength = decimalLength32(value);  // Pre-compute string length

    // Process 4 digits at a time for efficiency
    while (value >= 10000) {
        const uint32 c = value - 10000 * (value / 10000);
        const uint32 c0 = (c % 100) << 1;    // Last 2 digits * 2 (for table lookup)
        const uint32 c1 = (c / 100) << 1;    // Next 2 digits * 2 (for table lookup)

        char *pos = a + olength - i;
        value /= 10000;

        // Copy digit pairs from lookup table
        memcpy(pos - 2, DIGIT_TABLE + c0, 2);
        memcpy(pos - 4, DIGIT_TABLE + c1, 2);
        i += 4;
    }

    // Process remaining 2 digits
    if (value >= 100) {
        const uint32 c = (value % 100) << 1;
        char *pos = a + olength - i;
        value /= 100;
        memcpy(pos - 2, DIGIT_TABLE + c, 2);
        i += 2;
    }

    // Process final 1-2 digits
    if (value >= 10) {
        const uint32 c = value << 1;
        char *pos = a + olength - i;
        memcpy(pos - 2, DIGIT_TABLE + c, 2);
    } else {
        *a = (char)('0' + value);
    }

    return olength;
}
```