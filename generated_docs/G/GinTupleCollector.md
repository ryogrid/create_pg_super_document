# GinTupleCollector

## Location
src/include/access/gin_private.h: 453 - 459

## Overview
A structure used during GIN (Generalized Inverted Index) fast insertion to collect and manage temporary index tuples before writing them to the pending list.

## Definition
```c
typedef struct GinTupleCollector
{
    IndexTuple *tuples;
    uint32      ntuples;
    uint32      lentuples;
    uint32      sumsize;
} GinTupleCollector;
```

## Detailed Description
GinTupleCollector is a core component of PostgreSQL's GIN index "fast insertion" mechanism. It serves as a temporary collection container for index tuples before they are written to the index's pending list. This approach optimizes performance by allowing batch operations instead of individual tuple insertions.

The fast insertion mechanism works by deferring the actual index tree modification until later (typically during VACUUM or when the pending list becomes too large). Instead, new index entries are collected in memory and then written to a linear pending list structure. This strategy significantly improves insertion performance for bulk operations at the cost of slightly more complex query processing (which must check both the main index structure and the pending list).

The collector uses a dynamically resizable array with power-of-2 growth strategy to efficiently manage memory allocation during the collection phase.

## Parameters / Member Variables
- `tuples`: Dynamic array of IndexTuple pointers containing the collected index tuples
- `ntuples`: Current number of valid tuples stored in the tuples array
- `lentuples`: Currently allocated capacity of the tuples array (always a power of 2)
- `sumsize`: Total size in bytes of all collected tuples (used for space management)

## Dependencies
- Functions called/Symbols referenced:
  - IndexTuple (PostgreSQL's index tuple structure)
  - Various memory management functions (palloc, repalloc)

- Called from (representative examples):
  - ginHeapTupleFastInsert (writes collected tuples to pending list)
  - ginHeapTupleFastCollect (adds new tuples to the collection)
  - gininsert (main insertion entry point in gininsert.c)

## Notes and Other Information
- Used exclusively during "fast insertion" mode in GIN indexes (ginfast.c)
- Memory allocation uses power-of-2 sizing strategy for efficiency (starts at 16, grows via pg_nextpower2_32)
- The sumsize field enables efficient space calculations for pending list management
- Collected tuples contain heap TIDs in their t_tid field for later processing
- All tuples for a single heap tuple must be collected and written together to maintain consistency
- Part of the deferred insertion strategy that trades immediate index maintenance for better bulk performance
- The collector is typically used temporarily during single insert operations, then flushed to the pending list