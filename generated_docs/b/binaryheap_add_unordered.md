# binaryheap_add_unordered

## Location
src/common/binaryheap.c: 116 - 137

## Overview
A function that adds a new element to the end of a binary heap's array without maintaining the heap property, designed for efficient bulk insertion followed by heap construction.

## Definition
void binaryheap_add_unordered(binaryheap *heap, bh_node_type d)

## Detailed Description
This function provides an O(1) method to add elements to a binary heap when the heap property doesn't need to be maintained immediately. It simply appends the new element to the end of the heap's node array and increments the size counter. The function sets the bh_has_heap_property flag to false to indicate that the heap structure is invalid and requires rebuilding via binaryheap_build() before any heap operations can be performed. This approach is particularly useful for initializing heaps with multiple elements efficiently, as it avoids the O(log n) cost of maintaining heap properties for each insertion.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure to add the element to
- `d`: The data element (bh_node_type) to be added to the heap

## Dependencies
- Functions called/Symbols referenced:
  - [binaryheap](binaryheap.md) (struct type)
  - bh_node_type (type definition)
  - FRONTEND (preprocessor macro for conditional compilation)
  - [pg_fatal](../p/pg_fatal.md) (frontend error function)
  - elog (backend error function)
- Called from (representative examples):
  - [gather_merge_init](../g/gather_merge_init.md) (in src/backend/executor/nodeGatherMerge.c)
  - ExecMergeAppend (in src/backend/executor/nodeMergeAppend.c)
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md) (in src/backend/postmaster/pgarch.c)
  - [ReorderBufferIterTXNInit](../R/ReorderBufferIterTXNInit.md) (in src/backend/replication/logical/reorderbuffer.c)
  - BufferSync (in src/backend/storage/buffer/bufmgr.c)
  - [TopoSort](../T/TopoSort.md) (in src/bin/pg_dump/pg_dump_sort.c)

## Notes and Other Information
- The function will terminate the program (pg_fatal in frontend, elog ERROR in backend) if the heap capacity is exceeded
- After using this function, binaryheap_build() must be called to restore the heap property before any other heap operations
- This is specifically designed for bulk initialization scenarios where multiple elements need to be added before using the heap
- The bh_has_heap_property flag serves as a debugging aid to catch improper usage of the heap while it's in an invalid state
- Used extensively throughout PostgreSQL for efficient initialization of priority queues in various subsystems