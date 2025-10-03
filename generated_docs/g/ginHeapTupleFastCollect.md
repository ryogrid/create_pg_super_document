# ginHeapTupleFastCollect

## Location
[src/backend/access/gin/ginfast.c:483-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginfast.c#L483-L553)

## Overview
Creates temporary index tuples for a single indexable item from a heap tuple and appends them to a collector array for subsequent bulk insertion into the GIN pending list.

## Definition

```c
void
ginHeapTupleFastCollect(GinState *ginstate,
						GinTupleCollector *collector,
						OffsetNumber attnum, Datum value, bool isNull,
						ItemPointer ht_ctid)
```
## Detailed Description
This function is responsible for converting a single attribute value from a heap tuple into one or more index tuples that will be stored in GIN's pending list. It extracts key values from the input using ginExtractEntries, dynamically manages memory allocation for the collector's tuple array using power-of-2 sizing for efficiency, and creates index tuples for each extracted key. The function protects against integer overflow and ensures the collector has sufficient capacity before adding tuples. Each created index tuple includes the heap tuple's TID for later reference during cleanup operations.

## Parameters / Member Variables
- `ginstate`: Pointer to GinState structure containing index configuration and operator information
- `collector`: Pointer to GinTupleCollector where created tuples will be stored
- `attnum`: Column number of the attribute being indexed
- `value`: The datum value to be indexed
- `isNull`: Boolean indicating whether the value is NULL
- `ht_ctid`: ItemPointer to the heap tuple being indexed

## Dependencies
- Functions called/Symbols referenced:
  - [ginExtractEntries](ginExtractEntries.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - palloc_array
  - repalloc_array
  - [GinFormTuple](../G/GinFormTuple.md)
  - IndexTupleSize
  - MaxAllocSize
- Called from (representative examples):
  - [gininsert](gininsert.md)

## Notes and Other Information
- Part of GIN's fast insertion mechanism for collecting tuples before bulk insertion
- Uses power-of-2 allocation strategy to minimize memory waste during array resizing
- Protects against integer overflow when calculating memory requirements
- Stores heap TID directly in index tuple's t_tid for pending list entries
- Maintains running totals of tuple count and total size in the collector
- Must be followed by ginHeapTupleFastInsert to actually write collected tuples
- Guarantees that all tuples for a single heap tuple are collected together for consistency
- Efficiently handles both initial allocation and dynamic expansion of tuple arrays

## Simplified Source

```c
// Simplified version of ginHeapTupleFastCollect
void ginHeapTupleFastCollect(GinState *ginstate,
                            GinTupleCollector *collector,
                            OffsetNumber attnum, Datum value, bool isNull,
                            ItemPointer ht_ctid)
{
    Datum *entries;
    GinNullCategory *categories;
    int32 i, nentries;

    // Extract key values from the input
    entries = ginExtractEntries(ginstate, attnum, value, isNull,
                               &nentries, &categories);

    // Check for overflow
    if (nentries < 0 ||
        collector->ntuples + nentries > MaxAllocSize / sizeof(IndexTuple))
        elog(ERROR, "too many entries for GIN index");

    // Allocate or expand tuple array
    if (collector->tuples == NULL) {
        // Initial allocation using power of 2
        collector->lentuples = pg_nextpower2_32(Max(16, nentries));
        collector->tuples = palloc_array(IndexTuple, collector->lentuples);
    } else if (collector->lentuples < collector->ntuples + nentries) {
        // Expand using power of 2
        collector->lentuples = pg_nextpower2_32(collector->ntuples + nentries);
        collector->tuples = repalloc_array(collector->tuples,
                                          IndexTuple, collector->lentuples);
    }

    // Create index tuples for each extracted key
    for (i = 0; i < nentries; i++) {
        IndexTuple itup;

        itup = GinFormTuple(ginstate, attnum, entries[i], categories[i],
                           NULL, 0, 0, true);
        itup->t_tid = *ht_ctid;
        collector->tuples[collector->ntuples++] = itup;
        collector->sumsize += IndexTupleSize(itup);
    }
}
```