# bitshiftleft

## Location
[src/backend/utils/adt/varbit.c:1392-1458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1392-L1458)

## Overview
Performs a left bit shift operation on a variable-length bit string, shifting bits towards the beginning of the string.

## Definition

```c
Datum
bitshiftleft(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum
bitshiftleft(PG_FUNCTION_ARGS)
{
    VarBit *arg = PG_GETARG_VARBIT_P(0);
    int32 shft = PG_GETARG_INT32(1);
    VarBit *result;
    int byte_shift, ishift, len;
    bits8 *p, *r;

    // Negative shift becomes right shift
    if (shft < 0) {
        if (shft < -VARBITMAXLEN)
            shft = -VARBITMAXLEN;
        return DirectFunctionCall2(bitshiftright,
                                  VarBitPGetDatum(arg),
                                  Int32GetDatum(-shft));
    }

    // Allocate result with same size as input
    result = (VarBit *) palloc(VARSIZE(arg));
    SET_VARSIZE(result, VARSIZE(arg));
    VARBITLEN(result) = VARBITLEN(arg);
    r = VARBITS(result);

    // If shifting all bits out, return all zeros
    if (shft >= VARBITLEN(arg)) {
        MemSet(r, 0, VARBITBYTES(arg));
        return result;
    }

    // Calculate byte and bit shifts
    byte_shift = shft / BITS_PER_BYTE;
    ishift = shft % BITS_PER_BYTE;
    p = VARBITS(arg) + byte_shift;

    if (ishift == 0) {
        // Byte-aligned shift: use fast memory copy
        len = VARBITBYTES(arg) - byte_shift;
        memcpy(r, p, len);
        MemSet(r + len, 0, byte_shift);
    } else {
        // Bit-level shift: combine adjacent bytes
        for (; p < VARBITEND(arg); r++) {
            *r = *p << ishift;
            if ((++p) < VARBITEND(arg))
                *r |= *p >> (BITS_PER_BYTE - ishift);
        }
        // Zero remaining bytes
        for (; r < VARBITEND(result); r++)
            *r = 0;
    }

    return result;
}
```