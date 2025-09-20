# ItemPointerData

## Location
[src/include/storage/itemptr.h:36-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L36-L47)

## Overview
ItemPointerData is a fundamental PostgreSQL data structure that serves as a pointer to an item within a disk page of a known file, commonly used for cross-links from indexes to their parent tables.

## Definition

```c
typedef struct ItemPointerData
{
	BlockIdData ip_blkid;
	OffsetNumber ip_posid;
}

/* If compiler understands packed and aligned pragmas, use those */
#if defined(pg_attribute_packed) && defined(pg_attribute_aligned)
			pg_attribute_packed()
			pg_attribute_aligned(2)
#endif
ItemPointerData;
```
## Detailed Description
ItemPointerData represents a physical location of a tuple within PostgreSQL's storage system. It consists of two components: a block identifier (ip_blkid) that specifies which disk block contains the item, and a position identifier (ip_posid) that indicates which entry in the line pointer (ItemIdData) array within that block points to the desired item.

This structure is critical for PostgreSQL's storage efficiency as it appears in every tuple header and index tuple header on disk. The design prioritizes space efficiency, targeting exactly six bytes (three int16 fields) to minimize storage overhead. The structure uses compiler-specific attributes to prevent padding that would increase its size to eight bytes.

The ItemPointerData structure enables PostgreSQL's heap storage model where data pages contain line pointer arrays that reference the actual tuple data, allowing for efficient tuple updates and space management.

## Parameters / Member Variables
- : A BlockIdData structure that identifies the specific disk block containing the target item
- : An OffsetNumber (uint16) representing a 1-based index into the line pointer array within the specified block

## Dependencies
- Functions called/Symbols referenced:
  - [BlockIdData](../B/BlockIdData.md) (block identifier structure)
  - OffsetNumber (typedef for uint16, represents position in line pointer array)
  - [pg_attribute_aligned](../p/pg_attribute_aligned.md) (compiler attribute for memory alignment)
  - pg_attribute_packed (compiler attribute to prevent structure padding)

- Called from (representative examples):
  - Used as ItemPointer typedef (pointer to ItemPointerData)
  - Referenced in heap tuple headers (t_ctid field)
  - Used in index tuple structures
  - Utilized throughout storage layer for tuple addressing

## Notes and Other Information
- The structure is designed to be exactly 6 bytes to minimize storage overhead
- Compiler attributes are used to prevent structure padding that would waste space
- ItemPointer is a typedef for ItemPointerData* (pointer to this structure)
- Special values are used for heap tuples, such as SpecTokenOffsetNumber for speculative insertion tokens
- The design reflects PostgreSQL's emphasis on storage efficiency, as this structure appears in every tuple header
- Critical for PostgreSQL's MVCC (Multi-Version Concurrency Control) implementation
- Located in src/include/storage/itemptr.h:36-47