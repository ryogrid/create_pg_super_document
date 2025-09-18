# index_deform_tuple

## Location
src/backend/access/common/indextuple.c: 456 - 478

## Overview
The `index_deform_tuple` function converts an IndexTuple into separate arrays of Datum values and null indicators, providing the inverse operation of index tuple formation.

## Definition
```c
void index_deform_tuple(IndexTuple tup, TupleDesc tupleDescriptor, Datum *values, bool *isnull)
```

## Detailed Description
This function decomposes an IndexTuple into its constituent attribute values and null indicators, storing them in caller-provided arrays. It serves as the counterpart to the index tuple formation functions and is nearly identical to `heap_deform_tuple()` but specifically designed for IndexTuples.

The function performs the following key operations:
1. Locates the null bitmap immediately after the IndexTupleData header
2. Calculates the data section offset using IndexInfoFindDataOffset
3. Delegates the actual deformation work to `index_deform_tuple_internal`

A notable characteristic is that IndexTuples should never have missing columns, unlike HeapTuples which may have columns that were added after tuple creation.

## Parameters
- `tup`: IndexTuple to be deformed into component values
- `tupleDescriptor`: TupleDesc describing the expected structure of the tuple
- `values`: Output array to store Datum values (caller must allocate sufficient space, INDEX_MAX_KEYS entries recommended)
- `isnull`: Output array to store null indicators corresponding to each value

## Dependencies
- Functions called/Symbols referenced:
  - IndexInfoFindDataOffset
  - IndexTupleHasNulls
  - index_deform_tuple_internal
- Data types used:
  - bits8 (for null bitmap access)
  - IndexTupleData
- Called from (representative examples):
  - index_truncate_tuple (src/backend/access/common/indextuple.c:596)
  - _bt_check_unique (src/backend/access/nbtree/nbtinsert.c:660)
  - StoreIndexTuple (src/backend/executor/nodeIndexonlyscan.c:281)
  - comparetup_index_btree_tiebreak (src/backend/utils/sort/tuplesortvariants.c:1546)

## Notes and Other Information
- Located in src/backend/access/common/indextuple.c:456-478
- This is a thin wrapper function that delegates to `index_deform_tuple_internal` for the actual implementation
- The caller is responsible for allocating sufficient storage for the output arrays (INDEX_MAX_KEYS entries should be adequate)
- Unlike HeapTuples, IndexTuples should never contain missing columns
- The function assumes the standard IndexTuple layout with the null bitmap immediately following the IndexTupleData header
- Used across various PostgreSQL subsystems including B-tree operations, executor nodes, and tuple sorting