# heap_free_minimal_tuple

## Location
[src/backend/access/common/heaptuple.c:1523-1534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1523-L1534)

## Overview
Frees the memory allocated for a MinimalTuple structure by calling pfree() on the minimal tuple pointer.

## Definition
```c
void heap_free_minimal_tuple(MinimalTuple mtup)
```

## Detailed Description
The `heap_free_minimal_tuple` function is a simple wrapper around PostgreSQL's memory management system that deallocates the memory occupied by a MinimalTuple. This function is the counterpart to `heap_form_minimal_tuple()` and other functions that create MinimalTuples. It simply calls `pfree()` on the provided MinimalTuple pointer, releasing the memory back to PostgreSQL's memory context system.

This function is essential for preventing memory leaks when working with minimal tuples, especially in query execution contexts where many temporary tuples may be created and destroyed during processing.

## Parameters / Member Variables
- `mtup`: A pointer to the MinimalTuple structure to be freed. The tuple must have been previously allocated through PostgreSQL's memory management system.

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [tts_minimal_clear](../t/tts_minimal_clear.md)
  - [ExecHashTableInsert](../E/ExecHashTableInsert.md)
  - [ExecParallelHashTableInsert](../E/ExecParallelHashTableInsert.md)
  - [ExecParallelHashTableInsertCurrentBatch](../E/ExecParallelHashTableInsertCurrentBatch.md)
  - [ExecHashSkewTableInsert](../E/ExecHashSkewTableInsert.md)
  - [ExecHashJoinImpl](../E/ExecHashJoinImpl.md)
  - [ExecParallelHashJoinPartitionOuter](../E/ExecParallelHashJoinPartitionOuter.md)
  - [writetup_heap](../w/writetup_heap.md)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- This is the minimal tuple equivalent of `heap_freetuple()` for regular HeapTuples
- Commonly used in query execution, particularly in hash operations and tuple stores
- The function assumes the MinimalTuple was allocated using PostgreSQL's memory context system
- Critical for memory management in executor nodes that work with minimal tuples
- Used extensively in hash joins, tuple stores, and other executor operations where minimal tuples provide space efficiency
- Must be called to prevent memory leaks when minimal tuples are no longer needed

## Simplified Source

```c
void heap_free_minimal_tuple(MinimalTuple mtup) {
    // Simply free the minimal tuple memory
    pfree(mtup);
}
```