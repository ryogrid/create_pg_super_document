# TuplesortIndexHashArg

## Location
[src/backend/utils/sort/tuplesortvariants.c:138-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L138-L149)

## Overview
A data structure that extends TuplesortIndexArg with hash-specific fields for sorting hash index tuples, providing masks and bucket information needed for hash index construction.

## Definition
```c
typedef struct
{
    TuplesortIndexArg index;
    
    uint32        high_mask;        /* masks for sortable part of hash code */
    uint32        low_mask;
    uint32        max_buckets;
} TuplesortIndexHashArg;
```

## Detailed Description
TuplesortIndexHashArg is a specialized data structure used by PostgreSQL's tuple sorting mechanism for hash index creation. It inherits the basic index sorting functionality from TuplesortIndexArg and adds specific fields needed for hash index operations. This structure is pointed to by TuplesortPublic.arg in the index_hash subcase and is used exclusively by IndexTuple routines during hash index construction.

The structure provides essential hash-related parameters including high and low masks for the sortable portion of hash codes, and the maximum number of buckets. These fields enable the sorting system to properly handle hash values during the index build process, ensuring that tuples are sorted according to their hash codes in a way that facilitates efficient hash index construction.

## Parameters / Member Variables
- `index`: Base TuplesortIndexArg structure containing heapRel (table being indexed) and indexRel (index being built)
- `high_mask`: 32-bit mask used for the sortable part of hash codes, typically used for higher-order bits
- `low_mask`: 32-bit mask used for the sortable part of hash codes, typically used for lower-order bits
- `max_buckets`: Maximum number of buckets that the hash index can contain

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortIndexArg (base structure)
- Called from (representative examples):
  - [tuplesort_begin_index_hash](../t/tuplesort_begin_index_hash.md) (src/backend/utils/sort/tuplesortvariants.c:450, 453)
  - [comparetup_index_hash](../c/comparetup_index_hash.md) (src/backend/utils/sort/tuplesortvariants.c:1598)

## Notes and Other Information
- This structure is specific to the index_hash sorting subcase and is not used for other index types
- The structure is set by tuplesort_begin_index_hash function and used only by IndexTuple routines
- The mask fields are crucial for determining how hash codes are partitioned and sorted during index construction
- Part of PostgreSQL's tuple sorting variants system located in src/backend/utils/sort/tuplesortvariants.c
- Hash indexes in PostgreSQL use these parameters to organize data into buckets for efficient retrieval