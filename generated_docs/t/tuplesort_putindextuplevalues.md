# tuplesort_putindextuplevalues

## Location
[src/backend/utils/sort/tuplesortvariants.c:752-787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L752-L787)

## Overview
Creates an IndexTuple from provided values and adds it to the tuplesort, used during index creation to sort index entries before writing them to disk.

## Definition
```c
void tuplesort_putindextuplevalues(Tuplesortstate *state, Relation rel, ItemPointer self, const Datum *values, const bool *isnull)
```

## Detailed Description
This function constructs an IndexTuple from arrays of attribute values and null indicators, then adds it to the tuplesort for index building operations. It uses the index_form_tuple_context function to create the tuple in the appropriate memory context, sets the tuple identifier (TID) from the provided ItemPointer, and extracts the first sort key value for optimization. The function is specifically designed for index creation workflows where individual tuples need to be constructed from component values and then sorted before being written to the index structure.

## Parameters / Member Variables
- `state`: The tuplesort state object managing the sort operation
- `rel`: Relation descriptor for the index being built
- `self`: ItemPointer (TID) to be stored in the index tuple
- `values`: Array of Datum values for the index attributes
- `isnull`: Array of boolean flags indicating which values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [index_form_tuple_context](../i/index_form_tuple_context.md)
  - RelationGetDescr
  - [index_getattr](../i/index_getattr.md)
  - TupleSortUseBumpTupleCxt
  - GetMemoryChunkSpace
  - tuplesort_puttuple_common
- Called from (representative examples):
  - [gistSortedBuildCallback](../g/gistSortedBuildCallback.md)
  - [_h_spool](../h/_h_spool.md)
  - [_bt_spool](../b/_bt_spool.md)

## Notes and Other Information
- Constructs IndexTuple using index_form_tuple_context for proper memory context allocation
- Sets the tuple TID (t_tid) from the provided ItemPointer
- Extracts the first attribute value (datum1) for sort optimization using index_getattr
- Calculates tuple size using INDEX_SIZE_MASK for bump contexts or GetMemoryChunkSpace for standard contexts
- Supports abbreviation optimization when sort keys and converter are available
- Primarily used in index build operations for B-tree, hash, and GiST indexes
- Part of the standard interface for adding constructed index tuples to sorts during index creation