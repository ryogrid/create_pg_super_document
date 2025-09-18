# ReorderBufferReturnTupleBuf

## Location
src/backend/replication/logical/reorderbuffer.c: 606 - 620

## Overview
Frees a HeapTuple that was previously allocated by ReorderBufferGetTupleBuf().

## Definition
```c
void ReorderBufferReturnTupleBuf(HeapTuple tuple)
```

## Detailed Description
ReorderBufferReturnTupleBuf is a simple wrapper function that deallocates memory for a HeapTuple structure that was previously allocated by ReorderBufferGetTupleBuf(). The function performs a straightforward pfree() call to release the memory back to the system. This function serves as the counterpart to ReorderBufferGetTupleBuf() and maintains the abstraction layer for tuple memory management in the reorder buffer system.

## Parameters / Member Variables
- `tuple`: The HeapTuple to be freed (must have been allocated by ReorderBufferGetTupleBuf)

## Dependencies
- Functions called/Symbols referenced:
  - pfree
- Called from (representative examples):
  - ReorderBufferReturnChange

## Notes and Other Information
This function should only be called on HeapTuple objects that were allocated using ReorderBufferGetTupleBuf(). The function assumes that the HeapTuple was allocated as a single memory block containing both the HeapTuple structure and its associated data, which is the allocation pattern used by ReorderBufferGetTupleBuf(). It's part of the memory management pair for tuple handling in logical replication.