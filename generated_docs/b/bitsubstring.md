# bitsubstring

## Location
[src/backend/utils/adt/varbit.c:1055-1152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1055-L1152)

## Overview
The bitsubstring function implements the core bit string substring extraction logic in PostgreSQL, handling boundary conditions, memory allocation, and bit-level operations for both fixed-length and variable-length substring operations.

## Definition

```c
static VarBit *
bitsubstring(VarBit *arg, int32 s, int32 l, bool length_not_specified)
```
## Detailed Description
This internal function performs the actual substring extraction from bit strings with comprehensive boundary checking and bit manipulation. It handles both cases where a length is specified and where extraction continues to the end of the string. The function includes sophisticated logic for dealing with bit-level alignment when the substring doesn't start on a byte boundary, requiring bit shifting operations. It also implements SQL99 compliance for error conditions such as negative lengths and overflow protection when calculating end positions.

## Parameters / Member Variables
- : VarBit pointer to the source bit string
- : int32 - the 1-based starting position for extraction
- : int32 - the length of the substring to extract (or -1 for no-length variant)
- : bool - flag indicating whether this is a no-length extraction (extract to end)
- Returns: VarBit pointer to the newly allocated substring result

## Dependencies
- Functions called/Symbols referenced:
  - VARBITLEN (gets bit length of VarBit)
  - VARBITTOTALLEN (calculates total byte length needed)
  - VARBITHDRSZ (VarBit header size constant)
  - VARBITS (gets pointer to bit data)
  - VARBITEND (gets pointer to end of bit data)
  - VARBIT_PAD (ensures proper padding of last byte)
  - SET_VARSIZE (sets PostgreSQL variable-length data size)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - memcpy (memory copy operation)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (overflow-safe addition)
  - ereport/ERROR (PostgreSQL error reporting)
  - Max/Min (utility macros)
- Called from:
  - [bitsubstr](bitsubstr.md) (at src/backend/utils/adt/varbit.c:1040)
  - [bitsubstr_no_len](bitsubstr_no_len.md) (at src/backend/utils/adt/varbit.c:1049)
  - [bit_overlay](bit_overlay.md) (at src/backend/utils/adt/varbit.c:1197, 1198)

## Notes and Other Information
- This is a static function defined in src/backend/utils/adt/varbit.c at lines 1055-1152
- Implements SQL99 compliance for substring error conditions
- Handles both byte-aligned and bit-aligned substring extraction
- Uses bit shifting operations (ishift = (s1-1) % BITS_PER_BYTE) for non-byte-aligned extractions
- Includes overflow protection using pg_add_s32_overflow for large length values
- Returns zero-length bit strings for invalid ranges rather than errors
- Ensures proper zero-padding of the last byte using VARBIT_PAD
- Converts from 1-based SQL indexing to 0-based C indexing internally
- Optimizes byte-boundary cases by using memcpy instead of bit-by-bit copying

## Simplified Source

```c
static VarBit *bitsubstring(VarBit *arg, int32 s, int32 l, bool length_not_specified) {
    int bitlen = VARBITLEN(arg);
    int32 s1 = Max(s, 1);  // Ensure start position is at least 1
    int32 e1;

    // Determine end position
    if (length_not_specified) {
        e1 = bitlen + 1;  // Extract to end of string
    } else if (l < 0) {
        ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR),
                       errmsg("negative substring length not allowed")));
    } else {
        int32 e;
        if (pg_add_s32_overflow(s, l, &e))
            e1 = bitlen + 1;  // Overflow case: extract to end
        else
            e1 = Min(e, bitlen + 1);
    }

    // Check for zero-length result
    if (s1 > bitlen || e1 <= s1) {
        int len = VARBITTOTALLEN(0);
        VarBit *result = (VarBit *) palloc(len);
        SET_VARSIZE(result, len);
        VARBITLEN(result) = 0;
        return result;
    }

    // Extract substring
    int rbitlen = e1 - s1;
    int len = VARBITTOTALLEN(rbitlen);
    VarBit *result = (VarBit *) palloc(len);
    SET_VARSIZE(result, len);
    VARBITLEN(result) = rbitlen;
    len -= VARHDRSZ + VARBITHDRSZ;

    // Copy data - optimize for byte boundaries
    if ((s1 - 1) % BITS_PER_BYTE == 0) {
        // Byte-aligned copy
        memcpy(VARBITS(result), VARBITS(arg) + (s1 - 1) / BITS_PER_BYTE, len);
    } else {
        // Bit-level copy with shifting
        int ishift = (s1 - 1) % BITS_PER_BYTE;
        bits8 *r = VARBITS(result);
        bits8 *ps = VARBITS(arg) + (s1 - 1) / BITS_PER_BYTE;

        for (int i = 0; i < len; i++) {
            *r = (*ps << ishift) & BITMASK;
            if ((++ps) < VARBITEND(arg))
                *r |= *ps >> (BITS_PER_BYTE - ishift);
            r++;
        }
    }

    VARBIT_PAD(result);  // Ensure proper padding
    return result;
}
```