# bitsetbit

## Location
src/backend/utils/adt/varbit.c: 1807 - 1868

## Overview
Creates a new bit string by setting a specific bit position to a given value (0 or 1) in a PostgreSQL variable-length bit string.

## Definition
Datum bitsetbit(PG_FUNCTION_ARGS)

## Detailed Description
This function takes an existing bit string and creates a modified copy where a single bit at a specified position has been set to a new value. The bit indexing is zero-based and counted from left to right, which is consistent with other get_bit and set_bit functions but differs from standard PostgreSQL substring and position functions.

The function performs input validation to ensure the bit position is within valid bounds and that the new bit value is either 0 or 1. It creates a complete copy of the original bit string and then modifies only the target bit using bitwise operations.

The bit manipulation involves calculating the target byte position and the bit position within that byte, then using bitwise AND/OR operations to clear or set the specific bit without affecting other bits in the same byte.

## Parameters / Member Variables
- `arg1`: The original bit string (PG_GETARG_VARBIT_P(0))
- `n`: Zero-based bit position to modify (PG_GETARG_INT32(1))
- `newBit`: New bit value, must be 0 or 1 (PG_GETARG_INT32(2))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P
  - PG_GETARG_INT32
  - VARBITLEN
  - VARSIZE
  - palloc
  - SET_VARSIZE
  - VARBITS
  - VARBITBYTES
  - memcpy
  - BITS_PER_BYTE
  - PG_RETURN_VARBIT_P
  - ereport (for error handling)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses zero-based indexing from left to right
- Creates a new bit string rather than modifying the original in-place
- Validates bit position is within range [0, bitlen-1]
- Validates new bit value is exactly 0 or 1
- Throws ERRCODE_ARRAY_SUBSCRIPT_ERROR for invalid bit positions
- Throws ERRCODE_INVALID_PARAMETER_VALUE for invalid bit values
- Bit position calculation: byteNo = n / BITS_PER_BYTE, bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE)
- Located in src/backend/utils/adt/varbit.c:1807-1868