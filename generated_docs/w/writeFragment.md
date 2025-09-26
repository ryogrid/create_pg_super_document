# writeFragment

## Location
[src/backend/access/transam/generic_xlog.c:90-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/generic_xlog.c#L90-L120)

## Overview
Writes the next fragment into a PageData structure's delta buffer, storing the offset, length, and actual data of a modified region within a database page.

## Definition

```c
static void
writeFragment(PageData *pageData, OffsetNumber offset, OffsetNumber length,
			  const char *data)
```
## Detailed Description
This function is a core component of PostgreSQL's generic WAL logging mechanism. It appends a fragment (representing a contiguous modified region of a page) to the delta buffer within a PageData structure. The fragment consists of three parts written sequentially: the offset within the page, the length of the modified data, and the actual data bytes. This compact format allows efficient storage and reconstruction of page modifications during WAL replay.

The function performs bounds checking via Assert to ensure the delta buffer has sufficient space, then uses memcpy operations to write the fragment components in binary format. The deltaLen field is updated to reflect the new buffer position after writing.

## Parameters / Member Variables
- : Pointer to PageData structure containing the delta buffer where the fragment will be written
- : OffsetNumber specifying the byte offset within the page where this fragment's data belongs
- : OffsetNumber specifying the number of bytes of data in this fragment
- : Pointer to the actual data bytes to be written into the fragment

## Dependencies
- Functions called/Symbols referenced:
  - PageData (struct type)
  - memcpy (standard library function)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - [computeRegionDelta](../c/computeRegionDelta.md) (twice - lines 196 and 217)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the generic_xlog.c source file
- The function assumes the caller has already verified that sufficient space exists in the delta buffer
- Fragment format: [offset][length][data] - all stored as binary data
- Part of PostgreSQL's generic WAL logging system for custom access methods
- The delta buffer format enables efficient page reconstruction during crash recovery