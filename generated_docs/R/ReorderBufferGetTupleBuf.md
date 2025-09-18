# ReorderBufferGetTupleBuf

## Location
src/backend/replication/logical/reorderbuffer.c: 588 - 605

## Overview
Allocates and returns a fresh HeapTuple structure with sufficient memory to hold tuple data of a specified size.

## Definition
```c
HeapTuple ReorderBufferGetTupleBuf(ReorderBuffer *rb, Size tuple_len)
```

## Detailed Description
ReorderBufferGetTupleBuf allocates memory for a HeapTuple structure along with its associated tuple data in a single allocation. The function calculates the total allocation size by adding the tuple length to the HeapTupleHeader size, then allocates memory from the ReorderBuffer's tuple context. The returned HeapTuple has its t_data pointer properly initialized to point to the data portion of the allocation, which immediately follows the HeapTuple structure in memory.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer from which to allocate memory
- `tuple_len`: Size of the tuple data (excluding the HeapTupleHeader overhead)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - SizeofHeapTupleHeader
  - HEAPTUPLESIZE
  - HeapTupleHeader
- Called from (representative examples):
  - [DecodeInsert](../D/DecodeInsert.md)
  - [DecodeUpdate](../D/DecodeUpdate.md)
  - [DecodeDelete](../D/DecodeDelete.md)
  - [DecodeMultiInsert](../D/DecodeMultiInsert.md)
  - [ReorderBufferRestoreChange](ReorderBufferRestoreChange.md)

## Notes and Other Information
The function performs a single memory allocation that includes both the HeapTuple structure and the tuple data buffer. The t_data pointer is set to point immediately after the HeapTuple structure in the allocated memory block. This efficient allocation strategy minimizes memory fragmentation and improves performance in logical replication scenarios where many tuples are processed.