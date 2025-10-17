# pg_generic_charinc

## Location
[src/backend/utils/mb/mbutils.c:1325-1358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1325-L1358)

## Overview
A generic character incrementer function that finds the next valid character in an encoding by incrementing bytes and verifying validity, used as a fallback when no encoding-specific incrementer is available.

## Definition
```c
static bool pg_generic_charinc(unsigned char *charptr, int len)
```

## Detailed Description
This function implements a generic character incrementing algorithm that works with any multibyte encoding. It operates by incrementing the last byte of a character representation until it finds a validly-encoded character according to the current database encoding's character verifier function.

The algorithm is conservative and only increments the last (least significant) byte, avoiding the complexity and performance overhead of trying to increment higher-order bytes. This approach may not find all possible next characters in some encodings, but provides a reliable fallback mechanism.

The function uses the encoding-specific character verifier from pg_wchar_table to validate each candidate character as it increments the byte value.

## Parameters / Member Variables
- `charptr`: Pointer to the character bytes to increment (modified in place)
- `len`: Length of the character in bytes
- Returns: `true` if a valid next character was found, `false` if no valid character exists by incrementing the last byte

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) - gets the current database encoding ID
  - pg_wchar_table - global table containing encoding metadata and function pointers
  - mbverify (via pg_wchar_table) - encoding-specific character verification function

- Called from (representative examples):
  - [pg_database_encoding_character_incrementer](pg_database_encoding_character_incrementer.md) - returns this as the default incrementer (src/backend/utils/mb/mbutils.c:1538)

## Notes and Other Information
- This is a static function, only used within mbutils.c
- Used as the default/fallback character incrementer for encodings without specialized incrementers
- Only increments the last byte to avoid performance issues with wide characters
- Specialized incrementers exist for UTF8 (pg_utf8_increment) and EUC_JP (pg_eucjp_increment)
- The function signature matches the mbcharacter_incrementer function pointer type
- Function signature location: src/backend/utils/mb/mbutils.c:1325-1358
- Part of PostgreSQL's multibyte character handling infrastructure
- Byte value 255 is treated as the maximum, stopping the search there

## Simplified Source

```c
static bool pg_generic_charinc(unsigned char *charptr, int len) {
    unsigned char *last_byte = charptr + len - 1;

    // Get the character verifier function for current database encoding
    mbchar_verifier verify_func = pg_wchar_table[GetDatabaseEncoding()].mbverifychar;

    // Try incrementing the last byte until we find a valid character
    while (*last_byte < 255) {
        (*last_byte)++;

        // Check if this creates a valid character
        if (verify_func(charptr, len) == len)
            return true;
    }

    return false; // No valid next character found
}
```