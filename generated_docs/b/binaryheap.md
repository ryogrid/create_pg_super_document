# binaryheap

## Location
src/include/lib/binaryheap.h: 42 - 50

## Overview
The binaryheap structure represents a binary heap data structure implementation in PostgreSQL, providing an efficient priority queue with O(log n) insertion and removal operations.

## Definition


## Detailed Description
The binaryheap structure implements a complete binary tree stored as an array, where each parent node satisfies a heap property relative to its children as defined by the comparison function. This implementation supports both min-heap and max-heap configurations depending on the provided comparator function. The heap provides efficient operations for priority queue functionality commonly used in sorting algorithms, merge operations, and scheduling tasks within PostgreSQL.

The structure uses a flexible array member for the nodes, allowing for variable-capacity heaps allocated in a single memory block. The heap maintains both its current size and total capacity, along with a debugging flag to track heap property validity after unordered operations.

## Parameters / Member Variables
- : Current number of nodes stored in the heap
- : Total capacity of nodes that can be stored in the allocated space
- : Boolean flag used for debugging to track whether the heap property is maintained (set to false after unordered operations)
- : Function pointer to the comparison function that defines the heap ordering property
- : User-provided argument passed to the comparison function for additional context
- : Flexible array member containing the actual heap nodes, with type determined by compilation context (Datum for backend, void* for frontend)

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - bh_node_type
  - binaryheap_comparator

- Called from (representative examples):
  - binaryheap_allocate
  - binaryheap_reset
  - binaryheap_free
  - binaryheap_add
  - binaryheap_remove_first
  - MergeAppendState (in executor nodes)
  - GatherMergeState (in parallel query execution)
  - BufferSync (in buffer management)
  - ReorderBufferIterTXNState (in logical replication)

## Notes and Other Information
- The heap uses a dual API: Datum-based for backend code and void*-based for frontend code, determined by the FRONTEND compilation flag
- Nodes are stored in a complete binary tree layout: root at index 0, left child of node i at 2*i+1, right child at 2*i+2, parent of node i at (i-1)/2
- The comparison function determines heap type: for max-heap, return <0 if a<b, 0 if a==b, >0 if a>b; for min-heap, conditions are reversed
- The bh_has_heap_property flag is used for debugging to detect when unordered operations have been performed and a heap rebuild is needed
- Memory allocation includes space for the structure plus the node array in a single block for efficiency
- Commonly used in PostgreSQL for merge operations, parallel query coordination, buffer management, and logical replication ordering