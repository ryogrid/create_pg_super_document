# pg_utf8_increment

## Location
src/backend/utils/mb/mbutils.c: 1359 - 1436

## Overview
A UTF-8 character incrementer function that safely increments UTF-8 encoded character bytes to generate the next valid character in lexicographic order.

## Definition


## Detailed Description
This function implements UTF-8-specific logic for incrementing character values, which is essential for range operations and string comparisons in PostgreSQL. It handles the complex UTF-8 encoding rules where multi-byte characters must maintain specific byte value constraints.

The function works by incrementing the last byte that can be safely incremented without violating UTF-8 encoding rules:
- For single-byte characters (< 0x7F), it simply increments the byte
- For multi-byte characters, continuation bytes must be between 0x80-0xBF, and the first byte has specific ranges
- It includes special handling to avoid surrogate pair regions which are invalid in UTF-8
- It processes bytes from right to left, incrementing the rightmost byte that hasn't reached its maximum value

The function is designed for use in range operations where exhaustive enumeration isn't feasible, so it doesn't reset lower-order bytes to minimum values.

## Parameters / Member Variables
- : Pointer to the UTF-8 character bytes to be incremented
- : Length of the UTF-8 character in bytes (1-4 bytes supported)

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