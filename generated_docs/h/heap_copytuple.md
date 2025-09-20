# heap_copytuple

## Location
[src/backend/access/common/heaptuple.c:776-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L776-L801)

## Overview
Creates a complete copy of a HeapTuple including the tuple header and data, allocating the entire structure as a single palloc() block for memory efficiency.

## Definition

```c
HeapTuple
heap_copytuple(HeapTuple tuple)
```
## Detailed Description
The  function creates a deep copy of an existing HeapTuple. The function allocates a single memory block that contains both the HeapTuple management structure and the tuple data, ensuring efficient memory usage and cache locality. This is the primary function for duplicating heap tuples when the entire tuple structure needs to be copied.

The function performs validation on the input tuple and returns NULL if the tuple is invalid or has no data. For valid tuples, it allocates memory for the new tuple and copies all metadata (length, self-reference, table OID) as well as the complete tuple data using memcpy.

## Parameters / Member Variables
- : The source HeapTuple to be copied. Must be a valid tuple with non-NULL t_data field.

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleIsValid (macro for tuple validation)
  - [palloc](../p/palloc.md) (memory allocation)
  - memcpy (data copying)
  - HEAPTUPLESIZE (size constant)
  - HeapTupleHeader (type casting)
- Called from (representative examples):
  - [rewrite_heap_tuple](../r/rewrite_heap_tuple.md)
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - [MergeWithExistingConstraint](../M/MergeWithExistingConstraint.md)
  - [CopyStatistics](../C/CopyStatistics.md)
  - [tts_heap_materialize](../t/tts_heap_materialize.md)
  - [SPI_copytuple](../S/SPI_copytuple.md)
  - [SearchSysCacheCopy](../S/SearchSysCacheCopy.md)

## Notes and Other Information
- Returns NULL for invalid input tuples or tuples with NULL data
- Allocates memory as a single palloc() block containing both HeapTuple struct and data
- The resulting tuple is completely independent of the source tuple
- Used extensively throughout PostgreSQL for tuple duplication in catalog operations, executor operations, and SPI functions
- Located in src/backend/access/common/heaptuple.c:776-801