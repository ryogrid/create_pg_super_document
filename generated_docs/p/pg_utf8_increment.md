# pg_utf8_increment

## Location
[src/backend/utils/mb/mbutils.c:1359-1436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1359-L1436)

## Overview
A UTF-8 character incrementer function that safely increments UTF-8 encoded character bytes to generate the next valid character in lexicographic order.

## Definition

```c
static bool
pg_utf8_increment(unsigned char *charptr, int length)
```
## Detailed Description
This function implements UTF-8-specific logic for incrementing character values, which is essential for range operations and string comparisons in PostgreSQL. It handles the complex UTF-8 encoding rules where multi-byte characters must maintain specific byte value constraints.

The function works by incrementing the last byte that can be safely incremented without violating UTF-8 encoding rules:
- For single-byte characters (< 0x7F), it simply increments the byte
- For multi-byte characters, continuation bytes must be between 0x80-0xBF, and the first byte has specific ranges
- It includes special handling to avoid surrogate pair regions which are invalid in UTF-8
- It processes bytes from right to left, incrementing the rightmost byte that hasn't reached its maximum value

The function is designed for use in range operations where exhaustive enumeration isn't feasible, so it doesn't reset lower-order bytes to minimum values.

## Parameters / Member Variables
- `*charptr`: Pointer to the UTF-8 character bytes to be incremented
- `length`: Length of the UTF-8 character in bytes (1-4 bytes supported)
## Dependencies
- Functions called/Symbols referenced: None (uses only basic operations)
- Called from (representative examples):
  - [pg_database_encoding_character_incrementer](pg_database_encoding_character_incrementer.md)

## Notes and Other Information
- Rejects lengths 5 and 6 as they're not supported in standard UTF-8
- Handles special cases for surrogate pair avoidance (0xED prefix limited to 0x9F)
- Handles 4-byte character limits (0xF4 prefix limited to 0x8F)  
- Returns false if no valid increment is possible
- Critical for PostgreSQL's range type operations and string indexing performance

## Simplified Source

```c
static bool
pg_utf8_increment(unsigned char *charptr, int length)
{
    unsigned char current_byte;
    unsigned char limit;

    switch (length) {
        case 4:
            // Try to increment 4th byte (continuation byte)
            current_byte = charptr[3];
            if (current_byte < 0xBF) {
                charptr[3]++;
                return true;
            }
            // Fall through to try 3rd byte

        case 3:
            // Try to increment 3rd byte (continuation byte)
            current_byte = charptr[2];
            if (current_byte < 0xBF) {
                charptr[2]++;
                return true;
            }
            // Fall through to try 2nd byte

        case 2:
            // Try to increment 2nd byte with special limits
            current_byte = charptr[1];

            // Set limits based on first byte to avoid invalid UTF-8
            if (*charptr == 0xED)      // Surrogate pair avoidance
                limit = 0x9F;
            else if (*charptr == 0xF4) // 4-byte char limit
                limit = 0x8F;
            else
                limit = 0xBF;

            if (current_byte < limit) {
                charptr[1]++;
                return true;
            }
            // Fall through to try 1st byte

        case 1:
            // Try to increment first byte
            current_byte = *charptr;

            // Check if at boundary values that can't be incremented
            if (current_byte == 0x7F || current_byte == 0xDF ||
                current_byte == 0xEF || current_byte == 0xF4)
                return false;

            charptr[0]++;
            return true;

        default:
            // Reject unsupported lengths (5, 6+ bytes)
            return false;
    }
}
```