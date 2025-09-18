# datapagemap_iterator_t

## Location
[src/bin/pg_rewind/datapagemap.h:22-29](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/datapagemap.h#L22-L29)

## Overview
A typedef for the datapagemap_iterator struct, providing an iterator interface for traversing through data pages marked in a datapagemap bitmap.

## Definition
```c
typedef struct datapagemap_iterator datapagemap_iterator_t;

// The underlying struct definition:
struct datapagemap_iterator
{
	datapagemap_t *map;
	BlockNumber nextblkno;
};
```

## Detailed Description
The datapagemap_iterator_t provides a convenient way to iterate through all the data pages that are marked in a datapagemap bitmap. This iterator pattern allows sequential access to block numbers that have been flagged for processing during pg_rewind operations. The iterator maintains state information including a reference to the source datapagemap and the current position (next block number to be examined). This design enables efficient traversal of sparse bitmaps without requiring the caller to understand the internal bitmap representation.

## Parameters / Member Variables
- `map`: Pointer to the datapagemap_t structure being iterated over, providing access to the bitmap data
- `nextblkno`: BlockNumber indicating the next block number to examine during iteration, maintaining the current position state

## Dependencies
- Functions called/Symbols referenced:
  - [datapagemap_iterator](datapagemap_iterator.md) (references the underlying struct)
  - [datapagemap_t](datapagemap_t.md) (type used for map member)
  - BlockNumber (type used for nextblkno member)
- Called from (representative examples):
  - [datapagemap_iterate](datapagemap_iterate.md) (creates and returns iterator instances)
  - [datapagemap_next](datapagemap_next.md) (advances iterator and retrieves next block number)
  - [calculate_totals](../c/calculate_totals.md) (uses iterator for calculating rewind statistics)
  - [perform_rewind](../p/perform_rewind.md) (uses iterator during actual rewind operations)

## Notes and Other Information
- Part of pg_rewind's page tracking and processing system
- Implements the iterator pattern for efficient bitmap traversal
- The iterator is created by datapagemap_iterate() and advanced by datapagemap_next()
- Used in critical pg_rewind operations for identifying pages that need synchronization
- Typedef definition located in src/bin/pg_rewind/datapagemap.h:22
- Underlying struct definition located in src/bin/pg_rewind/datapagemap.c:18-22
- Provides abstraction over the bitmap implementation details