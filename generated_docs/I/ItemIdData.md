# ItemIdData

## Location
[src/include/storage/itemid.h:25-30](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemid.h#L25-L30)

## Overview
ItemIdData is a fundamental data structure in PostgreSQL's buffer page management system that represents a line pointer, providing indirection to tuples stored on a buffer page.

## Definition


## Detailed Description
ItemIdData serves as a line pointer that provides a level of indirection for accessing tuples on a buffer page. This structure is essential for PostgreSQL's page layout and tuple management. The structure uses bit fields to pack three pieces of information into a compact 4-byte structure: the offset to the tuple data, flags indicating the state of the line pointer, and the length of the tuple.

The line pointer system allows PostgreSQL to efficiently manage variable-length tuples on pages and supports advanced features like HOT (Heap-Only Tuples) optimization through redirect pointers. When a line pointer is "in use" but has no associated storage (lp_len == 0), it follows the convention that lp_len is always 0 regardless of the lp_flags state.

## Parameters / Member Variables
- : 15-bit field containing the offset to the tuple from the start of the page (0-32767 bytes)
- : 2-bit field indicating the state of the line pointer (LP_UNUSED=0, LP_NORMAL=1, LP_REDIRECT=2, LP_DEAD=3)
- : 15-bit field containing the byte length of the tuple (0-32767 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - LP_UNUSED (constant)
  - LP_NORMAL (constant)
  - LP_REDIRECT (constant)
  - LP_DEAD (constant)
- Called from (representative examples):
  - PageAddItemExtended
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - BloomMaxFilterSize
  - BrinMaxItemSize
  - GinMaxItemSize
  - BTMaxItemSize
  - MaxHeapTupleSize

## Notes and Other Information
- The structure is defined in src/include/storage/itemid.h:25-30
- A typedef alias  is defined as  for pointer access
- The bit field layout allows the entire structure to fit in 32 bits (4 bytes)
- Multiple macro functions are provided for manipulating ItemIdData: ItemIdGetLength, ItemIdGetOffset, ItemIdGetFlags, ItemIdSetNormal, ItemIdSetUnused, etc.
- The structure is fundamental to PostgreSQL's MVCC implementation and page-level storage management
- Used extensively across all major access methods (heap, btree, gin, gist, hash, spgist)
- Critical for space management calculations in various PostgreSQL subsystems