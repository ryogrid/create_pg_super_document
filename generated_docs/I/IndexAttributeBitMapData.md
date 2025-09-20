# IndexAttributeBitMapData

## Location
[src/include/access/itup.h:55-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/itup.h#L55-L58)

## Overview
IndexAttributeBitMapData is a structure that provides a bitmap to track null values in index tuples, used when the HasNulls bit is set in the IndexTupleData header.

## Definition

```c
typedef struct IndexAttributeBitMapData
{
	bits8		bits[(INDEX_MAX_KEYS + 8 - 1) / 8];
}			IndexAttributeBitMapData;
```
## Detailed Description
IndexAttributeBitMapData serves as a null bitmap for index tuples in PostgreSQL. This structure is only present in an index tuple when the HasNulls bit (bit 15) is set in the IndexTupleData.t_info field, indicating that the tuple contains one or more null attribute values.

The structure uses a compact bitmap representation where each bit corresponds to one index attribute, with the bit being set (1) if the corresponding attribute is null, and clear (0) if the attribute has a value. This allows efficient storage and quick null checking for index attributes.

The bitmap is sized to accommodate the maximum number of index keys supported by PostgreSQL (INDEX_MAX_KEYS = 32), using a byte-aligned allocation. The expression  ensures proper rounding up to the nearest byte boundary, resulting in exactly 4 bytes (32 bits) for the current maximum of 32 index keys.

This design philosophy balances space efficiency with simplicity - the bitmap size is fixed regardless of the actual number of attributes in a specific index, avoiding the need to store attribute count information and maintaining MAXALIGN constraints for optimal memory access patterns.

## Parameters / Member Variables
- : Array of bits8 (unsigned char) elements forming the null bitmap
  - Size:  bytes (currently 4 bytes for 32 maximum keys)
  - Bit value 1: corresponding attribute is null
  - Bit value 0: corresponding attribute has a value

## Dependencies
- Functions called/Symbols referenced:
  - bits8 (typedef for unsigned char, >= 8 bits)
  - INDEX_MAX_KEYS (constant defining maximum index attributes = 32)
- Called from (representative examples):
  - IndexAttributeBitMap (src/include/access/itup.h:60)
  - [IndexInfoFindDataOffset](IndexInfoFindDataOffset.md) (src/include/access/itup.h:104)
  - SGLTHDRSZ (src/include/access/spgist_private.h:408)

## Notes and Other Information
- This structure immediately follows IndexTupleData in memory when the HasNulls bit is set in t_info.
- The fixed size allocation (4 bytes for current INDEX_MAX_KEYS=32) provides consistent memory layout regardless of actual index attribute count.
- Changing INDEX_MAX_KEYS requires an initdb (database cluster initialization) as it affects on-disk storage format.
- The bitmap design allows for very efficient null checking using standard bitwise operations.
- The structure is part of the variable-length index tuple format, where IndexTupleData is followed by this bitmap (if needed), followed by the actual attribute values at MAXALIGN boundaries.
- Space efficiency: even indexes with fewer attributes use the full bitmap size, but this trade-off simplifies tuple format parsing and maintains alignment requirements.