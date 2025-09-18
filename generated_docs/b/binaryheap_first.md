# binaryheap_first

## Location
src/common/binaryheap.c: 177 - 191

## Overview
A function that returns the root element (highest/lowest priority element) of a binary heap without removing it, providing O(1) access to the top priority item.

## Definition
bh_node_type binaryheap_first(binaryheap *heap)

## Detailed Description
This function provides constant-time access to the root element of the binary heap, which is always stored at index 0 of the heap array. In a max-heap, this returns the largest element; in a min-heap, it returns the smallest element. The function includes assertions to ensure the heap is not empty and that the heap property is maintained, making it safe for debugging builds. This is a read-only operation that does not modify the heap structure, making it suitable for examining the top priority element before deciding whether to remove it.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure from which to retrieve the first element

## Dependencies
- Functions called/Symbols referenced:
  - [binaryheap](binaryheap.md) (struct type)
  - binaryheap_empty (macro to check if heap is empty)
  - bh_node_type (return type definition)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [gather_merge_getnext](../g/gather_merge_getnext.md) (in src/backend/executor/nodeGatherMerge.c)
  - ExecMergeAppend (in src/backend/executor/nodeMergeAppend.c)
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md) (in src/backend/postmaster/pgarch.c)
  - [ReorderBufferIterTXNNext](../R/ReorderBufferIterTXNNext.md) (in src/backend/replication/logical/reorderbuffer.c)
  - BufferSync (in src/backend/storage/buffer/bufmgr.c)

## Notes and Other Information
- Always executes in O(1) constant time regardless of heap size
- The caller must ensure the heap is not empty before calling this function
- Includes debug assertions to verify heap validity (non-empty and heap property maintained)
- Does not modify the heap structure, making it safe to call multiple times
- Commonly used in priority queue implementations to peek at the next item to be processed
- The root element is always at index 0 due to the binary heap's array representation
- Used extensively in PostgreSQL's merge operations, buffer management, and replication systems where priority-based processing is required