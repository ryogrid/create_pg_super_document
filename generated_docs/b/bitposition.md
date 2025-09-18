# bitposition

## Location
src/backend/utils/adt/varbit.c: 1698 - 1806

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