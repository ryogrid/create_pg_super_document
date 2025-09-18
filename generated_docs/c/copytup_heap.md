# copytup_heap

## Location
src/backend/utils/sort/tuplestore.c: 1490 - 1499

## Overview
A specialized routine for copying HeapTuple data into tuplestore storage by converting it to the more compact MinimalTuple format and tracking memory usage.

## Definition
```c
static void *copytup_heap(Tuplestorestate *state, void *tup)
```

## Detailed Description
This function is part of the tuplestore machinery specialized for handling HeapTuple data. It converts a HeapTuple to a MinimalTuple, which is the actual storage format used internally by tuplestore. The conversion removes HeapTuple-specific overhead while preserving the essential tuple data. The function also updates the memory usage accounting by calling USEMEM to track the memory allocated for the new MinimalTuple. This approach maintains historical compatibility by allowing the COPYTUP interface to work with HeapTuple input while using the more efficient MinimalTuple storage format internally.

## Parameters / Member Variables
- `state`: The tuplestore state for memory accounting and context
- `tup`: Input HeapTuple to be copied (cast from void* for generic interface compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - Tuplestorestate
  - MinimalTuple
  - minimal_tuple_from_heap_tuple
  - GetMemoryChunkSpace
  - USEMEM (memory accounting macro)
- Called from (representative examples):
  - tuplestore_begin_heap

## Notes and Other Information
- This is a static function, only accessible within the tuplestore.c module
- Converts HeapTuple to MinimalTuple format for more efficient storage
- MinimalTuple format includes the length in its first word, eliminating the need to store length separately
- Memory usage tracking is automatically updated via USEMEM macro
- Returns a void* pointer to maintain generic interface compatibility, though the actual return type is MinimalTuple
- Part of the specialized routines for HeapTuple case, providing historical compatibility while using modern storage formats