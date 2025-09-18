# bitshiftleft

## Location
[src/backend/utils/adt/varbit.c:1392-1458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1392-L1458)

## Overview
Performs a left bit shift operation on a variable-length bit string, shifting bits towards the beginning of the string.

## Definition


## Detailed Description
The  function implements left bit shifting for PostgreSQL's variable-length bit string data type (varbit). It takes a bit string and an integer shift amount, and returns a new bit string with all bits shifted left by the specified number of positions. The function handles both byte-aligned and bit-aligned shifts efficiently.

Key behaviors:
- Negative shift values are converted to right shifts by calling 
- If the shift amount exceeds the bit string length, returns an all-zero string
- Optimizes byte-aligned shifts using 
- For non-byte-aligned shifts, performs bit-level manipulation
- Preserves the original bit string length in the result

## Parameters / Member Variables
- : The input variable-length bit string (VarBit) to be shifted
- : The number of bit positions to shift left (int32)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (extract bit string argument)
  - PG_GETARG_INT32 (extract integer argument)
  - DirectFunctionCall2 (call bitshiftright for negative shifts)
  - [palloc](../p/palloc.md) (allocate memory for result)
  - SET_VARSIZE/VARSIZE (manage variable-length object size)
  - VARBITLEN (get/set bit string length)
  - VARBITS (access bit data)
  - VARBITBYTES (calculate byte length)
  - VARBITEND (get end pointer)
  - MemSet (zero memory)
  - memcpy (copy memory)
  - PG_RETURN_VARBIT_P (return result)
- Called from (representative examples):
  - [bitshiftright](bitshiftright.md) (for handling negative left shifts)

## Notes and Other Information
- Implements the PostgreSQL SQL operator  for bit strings
- Handles edge cases like negative shifts and oversized shifts gracefully
- Uses bit manipulation techniques for efficient shifting at sub-byte boundaries
- The function maintains the invariant that pad bits in the result remain zero
- Maximum shift values are bounded by VARBITMAXLEN to prevent integer overflow