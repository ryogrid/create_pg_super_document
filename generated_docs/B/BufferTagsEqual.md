# BufferTagsEqual

## Location
[src/include/storage/buf_internals.h:154-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L154-L163)

## Overview
BufferTagsEqual is an inline function that performs field-by-field comparison between two BufferTag structures to determine if they refer to the same database block.

## Definition
static inline bool
BufferTagsEqual(const BufferTag *tag1, const BufferTag *tag2)

## Detailed Description
BufferTagsEqual provides an efficient mechanism to compare two BufferTag structures for equality. Since BufferTag serves as a unique identifier for database blocks within PostgreSQL's buffer management system, this comparison function is essential for buffer lookup operations, cache validation, and buffer management decisions.

The function performs a comprehensive comparison of all five fields that comprise a BufferTag: tablespace OID, database OID, relation number, block number, and fork number. All fields must match exactly for the function to return true, ensuring that the comparison identifies the exact same database block across the entire PostgreSQL instance.

This function is heavily used in buffer management operations where precise block identification is crucial, such as buffer validation, recent buffer checks, and buffer invalidation processes.

## Parameters / Member Variables
- : Pointer to the first BufferTag structure for comparison
- : Pointer to the second BufferTag structure for comparison

## Dependencies
- Functions called/Symbols referenced:
  - BufferTag (structure type)
- Called from (representative examples):
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [InvalidateVictimBuffer](../I/InvalidateVictimBuffer.md)
  - [LocalBufferAlloc](../L/LocalBufferAlloc.md)

## Notes and Other Information
- This is an inline function for performance optimization in frequent buffer operations
- Returns true only when all five BufferTag fields (spcOid, dbOid, relNumber, blockNum, forkNum) match exactly
- Critical for buffer cache correctness and integrity
- Used extensively in buffer lookup and validation operations
- The comparison order is optimized for early exit on mismatches
- Essential for ensuring buffer operations target the correct database blocks

## Simplified Source

```c
static inline bool
BufferTagsEqual(const BufferTag *tag1, const BufferTag *tag2)
{
    // Compare all BufferTag fields for exact match
    return (tag1->spcOid == tag2->spcOid) &&
           (tag1->dbOid == tag2->dbOid) &&
           (tag1->relNumber == tag2->relNumber) &&
           (tag1->blockNum == tag2->blockNum) &&
           (tag1->forkNum == tag2->forkNum);
}
```