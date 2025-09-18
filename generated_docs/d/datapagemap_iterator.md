# datapagemap_iterator

## Location
[src/bin/pg_rewind/datapagemap.c:18-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/datapagemap.c#L18-L31)

## Overview
The datapagemap_iterator struct provides an iterator mechanism for traversing a datapagemap bitmap to enumerate all blocks that have been marked as modified in pg_rewind operations.

## Definition


## Detailed Description
The datapagemap_iterator is an internal structure used by pg_rewind to iterate through all blocks that have been marked as changed in a datapagemap bitmap. This iterator maintains state for sequential traversal of set bits in the bitmap, allowing efficient enumeration of modified data pages. The iterator is designed to work with the datapagemap system which tracks data pages that have changed and need to be synchronized during database rewinding operations.

The iterator works by maintaining a pointer to the datapagemap and tracking the next block number to examine. It supports forward-only iteration through the bitmap, checking each bit position sequentially and returning block numbers where bits are set.

## Parameters / Member Variables
- : Pointer to the datapagemap_t structure containing the bitmap data and metadata
- : The next BlockNumber to examine during iteration, used to maintain the current position in the bitmap

## Dependencies
- Functions called/Symbols referenced:
  - [datapagemap_t](datapagemap_t.md) (struct type used for the bitmap data)
  - BlockNumber (typedef for block numbering)

- Called from (representative examples):
  - [datapagemap_iterate](datapagemap_iterate.md) (creates and initializes the iterator)
  - [datapagemap_next](datapagemap_next.md) (uses the iterator to find the next set bit)
  - [datapagemap_print](datapagemap_print.md) (debugging function that uses the iterator)

## Notes and Other Information
- This is an opaque structure defined in the source file and exposed only through a typedef in the header file
- The iterator is allocated dynamically using pg_malloc() and should be freed with pg_free() after use
- The iterator maintains forward-only traversal state and cannot be reset or used for bidirectional iteration
- Located in src/bin/pg_rewind/datapagemap.c:18-22, part of the pg_rewind utility for rewinding PostgreSQL databases
- The iterator pattern allows efficient traversal of sparse bitmaps without requiring knowledge of the internal bitmap structure