# bitposition

## Location
[src/backend/utils/adt/varbit.c:1698-1806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1698-L1806)

## Overview
Finds the position of a substring within a PostgreSQL bit string, returning a 1-based index or 0 if not found.

## Definition
Datum bitposition(PG_FUNCTION_ARGS)

## Detailed Description
This function implements substring search functionality for PostgreSQL bit strings, similar to the POSITION() function for other data types. It searches for the first occurrence of a bit substring (S2) within a larger bit string (S1) and returns the 1-based position where the match begins.

The algorithm performs a bit-by-bit comparison at each possible position in the main string. It uses sophisticated bit masking techniques to handle cases where the substring alignment does not fall on byte boundaries. The function accounts for padding bits at the end of both the main string and substring to ensure accurate matching.

Special cases:
- If the substring is longer than the main string, returns 0
- If the substring has zero length, returns 1 (following SQL standard)
- If no match is found, returns 0

## Parameters / Member Variables
- `str`: The main bit string to search within (PG_GETARG_VARBIT_P(0))
- `substr`: The bit substring to search for (PG_GETARG_VARBIT_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P
  - VARBITLEN
  - VARBITPAD
  - BITMASK
  - VARBITBYTES
  - VARBITS
  - VARBITEND
  - BITS_PER_BYTE
  - PG_RETURN_INT32
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Implements 1-based indexing consistent with SQL POSITION() functions
- Uses complex bit manipulation to handle non-byte-aligned substring searches
- Accounts for padding bits in variable-length bit strings
- Contains debug logging code (disabled by #if 0) for troubleshooting bit operations
- Performance scales with O(n*m*8) where n is string length and m is substring length
- Located in src/backend/utils/adt/varbit.c:1698-1806

## Simplified Source

```c
Datum bitposition(PG_FUNCTION_ARGS) {
    VarBit *str = PG_GETARG_VARBIT_P(0);
    VarBit *substr = PG_GETARG_VARBIT_P(1);

    // Get string lengths
    int substr_length = VARBITLEN(substr);
    int str_length = VARBITLEN(str);

    // Handle edge cases
    if (str_length == 0 || substr_length > str_length)
        PG_RETURN_INT32(0);

    if (substr_length == 0)
        PG_RETURN_INT32(1);

    // Search for substring at each possible position
    for (int byte_pos = 0; byte_pos < VARBITBYTES(str) - VARBITBYTES(substr) + 1; byte_pos++) {
        for (int bit_pos = 0; bit_pos < BITS_PER_BYTE; bit_pos++) {

            // Check if substring matches at this position
            bool is_match = true;
            bits8 *str_ptr = VARBITS(str) + byte_pos;

            // Compare each byte of substring with string
            for (bits8 *substr_ptr = VARBITS(substr);
                 is_match && substr_ptr < VARBITEND(substr);
                 substr_ptr++) {

                // Apply bit shifting and masking for alignment
                bits8 shifted_substr = *substr_ptr >> bit_pos;
                bits8 mask = BITMASK >> bit_pos;

                // Handle padding for last bytes
                if (substr_ptr == VARBITEND(substr) - 1) {
                    mask &= end_mask >> bit_pos;
                }

                // Compare bits
                is_match = ((shifted_substr ^ *str_ptr) & mask) == 0;
                str_ptr++;

                // Check remaining bits in next byte if needed
                if (str_ptr < VARBITEND(str) && bit_pos > 0) {
                    shifted_substr = *substr_ptr << (BITS_PER_BYTE - bit_pos);
                    mask = ~(BITMASK >> bit_pos);
                    is_match = is_match && ((shifted_substr ^ *str_ptr) & mask) == 0;
                }
            }

            if (is_match)
                PG_RETURN_INT32(byte_pos * BITS_PER_BYTE + bit_pos + 1);
        }
    }

    PG_RETURN_INT32(0);  // No match found
}
```