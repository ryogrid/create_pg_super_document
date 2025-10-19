# BlockRefTableComparator

## Location
[src/common/blkreftable.c:1152-1183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L1152-L1183)

## Overview
A static comparator function used for sorting BlockRefTableSerializedEntry objects, arranging them by tablespace OID, database OID, relation number, and fork number to match the on-disk tree structure.

## Definition
```c
static int BlockRefTableComparator(const void *a, const void *b)
```

## Detailed Description
This function implements a multi-level comparison for BlockRefTableSerializedEntry structures, establishing a hierarchical sort order that mirrors PostgreSQL's on-disk storage organization. The comparison follows a strict precedence: tablespace OID takes highest priority, followed by database OID, then relation number, and finally fork number. This ordering ensures that related database objects are grouped together in a way that optimizes access patterns and maintains consistency with PostgreSQL's internal storage layout.

The function returns standard comparator values: negative for a < b, zero for a == b, and positive for a > b, making it suitable for use with qsort and other standard sorting algorithms.

## Parameters / Member Variables
- `a`: Pointer to the first BlockRefTableSerializedEntry to compare
- `b`: Pointer to the second BlockRefTableSerializedEntry to compare

## Dependencies
- Functions called/Symbols referenced:
  - [BlockRefTableSerializedEntry](BlockRefTableSerializedEntry.md) (structure)
- Called from (representative examples):
  - [BlockRefTableWriter](BlockRefTableWriter.md)
  - [WriteBlockRefTable](../W/WriteBlockRefTable.md)

## Notes and Other Information
- Static function, only accessible within the blkreftable.c module
- Implements lexicographic ordering based on (spcOid, dbOid, relNumber, forknum)
- Tablespace OID is the primary sort key to optimize disk access patterns
- Used internally by sorting functions to organize block reference data
- Essential for maintaining consistent ordering in serialized block reference tables
- Follows PostgreSQL's standard comparator function signature for use with qsort

## Simplified Source

```c
static int
BlockRefTableComparator(const void *a, const void *b)
{
    const BlockRefTableSerializedEntry *sa = a;
    const BlockRefTableSerializedEntry *sb = b;

    // Compare by tablespace OID first (primary sort key)
    if (sa->rlocator.spcOid != sb->rlocator.spcOid)
        return (sa->rlocator.spcOid > sb->rlocator.spcOid) ? 1 : -1;

    // Compare by database OID second
    if (sa->rlocator.dbOid != sb->rlocator.dbOid)
        return (sa->rlocator.dbOid > sb->rlocator.dbOid) ? 1 : -1;

    // Compare by relation number third
    if (sa->rlocator.relNumber != sb->rlocator.relNumber)
        return (sa->rlocator.relNumber > sb->rlocator.relNumber) ? 1 : -1;

    // Compare by fork number last
    if (sa->forknum != sb->forknum)
        return (sa->forknum > sb->forknum) ? 1 : -1;

    return 0; // All fields are equal
}
```