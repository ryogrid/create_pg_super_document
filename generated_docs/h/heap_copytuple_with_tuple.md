# heap_copytuple_with_tuple

## Location
[src/backend/access/common/heaptuple.c:802-827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L802-L827)

## Overview
Copies a tuple into a caller-supplied HeapTuple management struct, allowing for more controlled memory management compared to heap_copytuple.

## Definition


## Detailed Description
The  function copies tuple data from a source HeapTuple into a destination HeapTuple structure that is provided by the caller. Unlike , this function does not allocate the HeapTuple management structure itself - only the tuple data portion is allocated separately using palloc().

This function is useful when the caller wants to manage the HeapTuple structure allocation themselves, or when working with stack-allocated HeapTuple structures. The resulting tuple will have the same metadata and data as the source, but the memory layout differs from  as the HeapTuple struct and data are not allocated as a single block.

## Parameters / Member Variables
- : The source HeapTuple to copy from. Must be a valid tuple with non-NULL t_data field.
- : The destination HeapTuple structure (caller-allocated) where the copied data will be stored.

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleIsValid (macro for tuple validation)
  - [palloc](../p/palloc.md) (memory allocation for tuple data)
  - memcpy (data copying)
  - HeapTupleHeader (type casting)
- Called from (representative examples):
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Sets dest->t_data to NULL if the source tuple is invalid or has NULL data
- Only allocates memory for the tuple data portion, not the HeapTuple management structure
- The caller is responsible for managing the HeapTuple structure memory
- Results in a different memory layout compared to heap_copytuple() - not a single allocated block
- Used when more control over memory allocation is needed
- Located in src/backend/access/common/heaptuple.c:802-827