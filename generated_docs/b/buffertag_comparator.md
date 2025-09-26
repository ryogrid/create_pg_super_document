# buffertag_comparator

## Location
[src/backend/storage/buffer/bufmgr.c:5789-5822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5789-L5822)

## Overview
A static inline comparison function that compares two BufferTag structures to establish an ordering for buffer management operations in PostgreSQL.

## Definition

```c
static inline int
buffertag_comparator(const BufferTag *ba, const BufferTag *bb)
```
## Detailed Description
The buffertag_comparator function provides a three-way comparison between two BufferTag structures, returning a value indicating their relative ordering. This comparator is essential for sorting and organizing buffer tags in data structures like sorted arrays or binary trees used in buffer management.

The comparison is performed hierarchically:
1. First compares the RelFileLocator components using rlocator_comparator
2. Then compares the fork numbers if RelFileLocators are equal
3. Finally compares the block numbers if both RelFileLocators and fork numbers are equal

The function returns -1 if ba < bb, 0 if ba == bb, and 1 if ba > bb, following standard C comparison function conventions.

## Parameters / Member Variables
- : Pointer to the first BufferTag to compare
- : Pointer to the second BufferTag to compare

## Dependencies
- Functions called/Symbols referenced:
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - [rlocator_comparator](../r/rlocator_comparator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - BufferTag (type)
- Called from (representative examples):
  - BufferIsPinned
  - ST_COMPARE (radix tree comparison macro)

## Notes and Other Information
- This is a static inline function for performance efficiency in buffer management operations
- The hierarchical comparison ensures a consistent total ordering of BufferTag structures
- Used primarily in radix tree implementations for efficient buffer lookup and management
- The comparison order prioritizes RelFileLocator, then fork number, then block number