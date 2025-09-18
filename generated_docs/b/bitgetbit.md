# bitgetbit

## Location
src/backend/utils/adt/varbit.c: 1869 - 1894

## Overview
Retrieves the value (0 or 1) of a specific bit at a given position in a PostgreSQL variable-length bit string.

## Definition
Datum bitgetbit(PG_FUNCTION_ARGS)

## Detailed Description
This function extracts the value of a single bit from a PostgreSQL bit string at a specified zero-based position. The bit indexing follows a left-to-right, zero-based convention that is consistent with other get_bit and set_bit functions, though it differs from standard PostgreSQL substring and position functions.

The function performs bounds checking to ensure the requested bit position is valid for the given bit string. It then calculates which byte contains the target bit and determines the bit position within that byte. The bit value is extracted using bitwise AND operations with a bit mask.

The bit position calculation accounts for the fact that bits are stored in a big-endian fashion within each byte, with bit 0 being the leftmost (most significant) bit of the first byte.

## Parameters / Member Variables
- `arg1`: The bit string to read from (PG_GETARG_VARBIT_P(0))
- `n`: Zero-based bit position to retrieve (PG_GETARG_INT32(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P
  - PG_GETARG_INT32
  - VARBITLEN
  - VARBITS
  - BITS_PER_BYTE
  - PG_RETURN_INT32
  - ereport (for error handling)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses zero-based indexing from left to right
- Returns 1 if the bit is set, 0 if the bit is clear
- Validates bit position is within range [0, bitlen-1]
- Throws ERRCODE_ARRAY_SUBSCRIPT_ERROR for invalid bit positions
- Bit position calculation: byteNo = n / BITS_PER_BYTE, bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE)
- The bitNo calculation ensures proper bit ordering within bytes (leftmost bit = bit 0)
- Located in src/backend/utils/adt/varbit.c:1869-1894