# binaryheap_build

## Location
[src/common/binaryheap.c:138-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L138-L153)

## Overview
A function that transforms an array of unordered elements into a valid binary heap structure in O(n) time using the bottom-up heap construction algorithm.

## Definition
void binaryheap_build(binaryheap *heap)

## Detailed Description
This function implements the classic bottom-up heap construction algorithm, also known as Floyd's heap construction method. It iterates from the last non-leaf node (found using parent_offset of the last element) down to the root, calling sift_down on each node to ensure the heap property is maintained. This approach is more efficient than inserting elements one by one (which would be O(n log n)), achieving O(n) time complexity. The function is specifically designed to work with heaps that have been populated using binaryheap_add_unordered(), restoring the heap property and setting the bh_has_heap_property flag to true.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure that needs to be converted from an unordered array to a valid heap

## Dependencies
- Functions called/Symbols referenced:
  - [binaryheap](binaryheap.md) (struct type)
  - [parent_offset](../p/parent_offset.md) (calculates parent index in heap)
  - [sift_down](../s/sift_down.md) (maintains heap property by moving nodes downward)
- Called from (representative examples):
  - [gather_merge_init](../g/gather_merge_init.md) (in src/backend/executor/nodeGatherMerge.c)
  - ExecMergeAppend (in src/backend/executor/nodeMergeAppend.c)
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md) (in src/backend/postmaster/pgarch.c)
  - [ReorderBufferIterTXNInit](../R/ReorderBufferIterTXNInit.md) (in src/backend/replication/logical/reorderbuffer.c)
  - BufferSync (in src/backend/storage/buffer/bufmgr.c)
  - [TopoSort](../T/TopoSort.md) (in src/bin/pg_dump/pg_dump_sort.c)

## Notes and Other Information
- Uses the optimal O(n) bottom-up heap construction algorithm rather than O(n log n) top-down insertion
- Must be called after using binaryheap_add_unordered() to restore heap validity
- The algorithm starts from the last non-leaf node and works backwards to the root, ensuring all subtrees satisfy the heap property
- After completion, the heap is ready for standard heap operations like binaryheap_first(), binaryheap_add(), and binaryheap_remove_first()
- The bh_has_heap_property flag is set to true upon successful completion, enabling debug checks in other heap operations
- This is a fundamental operation used throughout PostgreSQL for efficiently initializing priority queues in various subsystems