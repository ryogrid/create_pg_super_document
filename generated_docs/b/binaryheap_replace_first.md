# binaryheap_replace_first

## Location
src/common/binaryheap.c: 255 - 269

## Overview
Replaces the topmost (root) element of a non-empty heap with a new value while preserving the heap property.

## Definition
```c
void binaryheap_replace_first(binaryheap *heap, bh_node_type d)
```

## Detailed Description
This function provides an efficient way to replace the root element of a binary heap without the overhead of separate remove and insert operations. It directly substitutes the new value at the root and then restores the heap property by sifting down if necessary. The operation is O(1) in the best case when the new element is already properly positioned, or O(log n) in the worst case when sifting is required.

The function performs the following steps:
1. Validates that the heap is not empty and has the heap property
2. Directly replaces the root element with the new value
3. If the heap has more than one element, calls sift_down to restore the heap property

This operation is more efficient than removing the first element and then inserting a new one, especially when the new element has a similar priority to the old root.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure
- `d`: The new node value to place at the root position

## Dependencies
- Functions called/Symbols referenced:
  - binaryheap_empty (heap validation)
  - [sift_down](../s/sift_down.md) (heap rebalancing downward)
- Called from (representative examples):
  - [gather_merge_getnext](../g/gather_merge_getnext.md) (executor/nodeGatherMerge.c:563)
  - ExecMergeAppend (executor/nodeMergeAppend.c:250)
  - [ReorderBufferIterTXNNext](../R/ReorderBufferIterTXNNext.md) (replication/logical/reorderbuffer.c:1448, 1484)
  - BufferSync (storage/buffer/bufmgr.c:3135)

## Notes and Other Information
- The caller must ensure the heap is not empty before calling this function
- More efficient than separate remove + insert operations for root replacement
- Commonly used in scenarios where the priority queue needs frequent updates to the top element
- Extensively used in PostgreSQL for query execution merge operations, replication buffering, and buffer management
- Only requires sifting if the heap has more than one element