# rlocator_comparator

## Location
[src/backend/storage/buffer/bufmgr.c:5708-5734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5708-L5734)

## Overview
A comparator function for RelFileLocator structures used in sorting and binary search operations within PostgreSQL buffer management.

## Definition
```c
static int rlocator_comparator(const void *p1, const void *p2)
```

## Detailed Description
This function implements a three-way comparison for RelFileLocator structures, following the standard qsort/bsearch comparator interface. It performs lexicographic comparison of the three key components of a RelFileLocator: relNumber (relation number), dbOid (database OID), and spcOid (tablespace OID). The comparison is performed in a specific order that establishes a total ordering suitable for sorting algorithms and binary search operations. The function returns -1 if the first locator is "less than" the second, 1 if "greater than", and 0 if they are equal according to the RelFileLocatorEquals semantics.

## Parameters / Member Variables
- `p1`: A void pointer to the first RelFileLocator to compare
- `p2`: A void pointer to the second RelFileLocator to compare

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (structure type)
- Called from (representative examples):
  - BufferIsPinned
  - [DropRelationsAllBuffers](../D/DropRelationsAllBuffers.md)
  - [FlushRelationsAllBuffers](../F/FlushRelationsAllBuffers.md)
  - [buffertag_comparator](../b/buffertag_comparator.md)

## Notes and Other Information
- This is a static function internal to bufmgr.c
- Follows the standard C library qsort/bsearch comparator contract
- Comparison order: relNumber first, then dbOid, then spcOid
- Used for efficiently sorting and searching arrays of RelFileLocator structures
- Essential for buffer management operations that need to process multiple relations efficiently
- The ordering established by this function must be consistent with RelFileLocatorEquals logic