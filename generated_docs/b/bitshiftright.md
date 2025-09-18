# bitshiftright

## Location
src/backend/utils/adt/varbit.c: 1459 - 1530

## Overview
Performs a right bit shift operation on a variable-length bit string, shifting bits towards the end of the string.

## Definition


## Detailed Description
The  function implements right bit shifting for PostgreSQL's variable-length bit string data type (varbit). It takes a bit string and an integer shift amount, and returns a new bit string with all bits shifted right by the specified number of positions. The function handles both byte-aligned and bit-aligned shifts efficiently.

Key behaviors:
- Negative shift values are converted to left shifts by calling 
- If the shift amount exceeds the bit string length, returns an all-zero string
- Optimizes byte-aligned shifts using 
- For non-byte-aligned shifts, performs bit-level manipulation
- Preserves the original bit string length in the result
- Ensures pad bits are correctly zeroed using 

## Parameters / Member Variables
- : The input variable-length bit string (VarBit) to be shifted
- : The number of bit positions to shift right (int32)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (extract bit string argument)
  - PG_GETARG_INT32 (extract integer argument)
  - DirectFunctionCall2 (call bitshiftleft for negative shifts)
  - [palloc](../p/palloc.md) (allocate memory for result)
  - SET_VARSIZE/VARSIZE (manage variable-length object size)
  - VARBITLEN (get/set bit string length)
  - VARBITS (access bit data)
  - VARBITBYTES (calculate byte length)
  - VARBITEND (get end pointer)
  - MemSet (zero memory)
  - memcpy (copy memory)
  - VARBIT_PAD_LAST (ensure pad bits are zero)
  - BITMASK (bit masking constant)
  - PG_RETURN_VARBIT_P (return result)
- Called from (representative examples):
  - [bitshiftleft](bitshiftleft.md) (for handling negative right shifts)

## Notes and Other Information
- Implements the PostgreSQL SQL operator  for bit strings
- Handles edge cases like negative shifts and oversized shifts gracefully
- Uses bit manipulation techniques for efficient shifting at sub-byte boundaries
- The function ensures pad bits remain zero by calling 
- Maximum shift values are bounded by VARBITMAXLEN to prevent integer overflow
- Right shifts require special handling to zero the leading bits correctly