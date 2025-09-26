# binaryheap_remove_first

## Location
[src/common/binaryheap.c:192-224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L192-L224)

## Overview
Removes the first (root, topmost) node from the binary heap and returns it after rebalancing the heap.

## Definition
```c
bh_node_type binaryheap_remove_first(binaryheap *heap)
```

## Detailed Description
This function implements the standard heap removal operation for a binary min-heap or max-heap structure. It extracts the root node (which contains either the minimum or maximum value depending on the heap type) and maintains the heap property by rebalancing the structure. The operation has O(log n) worst-case time complexity.

The function performs the following steps:
1. Validates that the heap is not empty and has the heap property
2. Extracts the root node as the return value
3. If the heap has only one element, simply decreases the size and returns
4. Otherwise, moves the last node to the root position and decreases heap size
5. Calls sift_down to restore the heap property by moving the new root to its correct position

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure from which to remove the first element

## Dependencies
- Functions called/Symbols referenced:
  - binaryheap_empty (heap validation)
  - [sift_down](../s/sift_down.md) (heap rebalancing)
- Called from (representative examples):
  - [gather_merge_getnext](../g/gather_merge_getnext.md) (executor/nodeGatherMerge.c:567)
  - [ExecMergeAppend](../E/ExecMergeAppend.md) (executor/nodeMergeAppend.c:252)
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md) (postmaster/pgarch.c:737, 761)
  - [ReorderBufferIterTXNNext](../R/ReorderBufferIterTXNNext.md) (replication/logical/reorderbuffer.c:1491)
  - [BufferSync](../B/BufferSync.md) (storage/buffer/bufmgr.c:3130)
  - [TopoSort](../T/TopoSort.md) (bin/pg_dump/pg_dump_sort.c:692)

## Notes and Other Information
- The caller must ensure the heap is not empty before calling this function
- The function assumes the heap has the heap property (heap->bh_has_heap_property)
- This is a destructive operation that modifies the heap structure
- The returned node should be handled appropriately by the caller
- Used extensively throughout PostgreSQL for priority queue operations in query execution, archiving, replication, and buffer management