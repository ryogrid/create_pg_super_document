# bit_catenate

## Location
[src/backend/utils/adt/varbit.c:977-1037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L977-L1037)

## Overview
The bit_catenate function performs the actual concatenation of two bit strings (VarBit types) in PostgreSQL, handling memory allocation, bit alignment, and padding.

## Definition


## Detailed Description
This internal function concatenates two variable-length bit strings by allocating a new result buffer and copying the bits from both input strings. It handles the complex task of bit alignment when the first string doesn't end on a byte boundary, requiring bit shifting operations to properly merge the second string. The function also validates that the combined length doesn't exceed PostgreSQL's maximum bit string length (VARBITMAXLEN).

## Parameters / Member Variables
- : VarBit pointer to the first bit string to concatenate
- : VarBit pointer to the second bit string to concatenate
- Returns: VarBit pointer to the newly allocated concatenated bit string

## Dependencies
- Functions called/Symbols referenced:
  - VARBITLEN (gets bit length of VarBit)
  - VARBITTOTALLEN (calculates total byte length needed)
  - VARBITBYTES (gets byte length of VarBit data)
  - VARBITPAD (gets padding bits in last byte)
  - VARBITS (gets pointer to bit data)
  - VARBITEND (gets pointer to end of bit data)
  - SET_VARSIZE (sets PostgreSQL variable-length data size)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - memcpy (memory copy operation)
  - ereport/ERROR (PostgreSQL error reporting)
- Called from:
  - [bitcat](bitcat.md) (at src/backend/utils/adt/varbit.c:973)
  - [bit_overlay](bit_overlay.md) (at src/backend/utils/adt/varbit.c:1199, 1200)

## Notes and Other Information
- This is a static function defined in src/backend/utils/adt/varbit.c at lines 977-1037
- Handles bit-level operations when concatenating strings that don't align on byte boundaries
- Implements overflow protection by checking against VARBITMAXLEN
- Uses bit shifting (bit2shift = BITS_PER_BYTE - bit1pad) to handle misaligned concatenation
- The function assumes that pad bits in the result are already zero after the bit manipulation operations
- Memory is allocated using palloc, which integrates with PostgreSQL's memory management system