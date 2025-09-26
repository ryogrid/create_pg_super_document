# binaryheap

## Location
[src/include/lib/binaryheap.h:42-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/binaryheap.h#L42-L50)

## Overview
The binaryheap structure represents a binary heap data structure implementation in PostgreSQL, providing an efficient priority queue with O(log n) insertion and removal operations.

## Definition

```c
typedef struct binaryheap
{
	int			bh_size;
	int			bh_space;
	bool		bh_has_heap_property;	/* debugging cross-check */
	binaryheap_comparator bh_compare;
	void	   *bh_arg;
	bh_node_type bh_nodes[FLEXIBLE_ARRAY_MEMBER];
} binaryheap;
```
## Detailed Description
The binaryheap structure implements a complete binary tree stored as an array, where each parent node satisfies a heap property relative to its children as defined by the comparison function. This implementation supports both min-heap and max-heap configurations depending on the provided comparator function. The heap provides efficient operations for priority queue functionality commonly used in sorting algorithms, merge operations, and scheduling tasks within PostgreSQL.

The structure uses a flexible array member for the nodes, allowing for variable-capacity heaps allocated in a single memory block. The heap maintains both its current size and total capacity, along with a debugging flag to track heap property validity after unordered operations.

## Parameters / Member Variables
- `bh_size`: Current number of nodes stored in the heap
- `bh_space`: Total capacity of nodes that can be stored in the allocated space
- `bh_has_heap_property`: Boolean flag used for debugging to track whether the heap property is maintained (set to false after unordered operations)
- `bh_compare`: Function pointer to the comparison function that defines the heap ordering property
- `*bh_arg`: User-provided argument passed to the comparison function for additional context
- `bh_nodes[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing the actual heap nodes, with type determined by compilation context (Datum for backend, void* for frontend)
## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - bh_node_type
  - binaryheap_comparator

- Called from (representative examples):
  - [binaryheap_allocate](binaryheap_allocate.md)
  - [binaryheap_reset](binaryheap_reset.md)
  - [binaryheap_free](binaryheap_free.md)
  - [binaryheap_add](binaryheap_add.md)
  - [binaryheap_remove_first](binaryheap_remove_first.md)
  - [MergeAppendState](../M/MergeAppendState.md) (in executor nodes)
  - [GatherMergeState](../G/GatherMergeState.md) (in parallel query execution)
  - [BufferSync](../B/BufferSync.md) (in buffer management)
  - [ReorderBufferIterTXNState](../R/ReorderBufferIterTXNState.md) (in logical replication)

## Notes and Other Information
- The heap uses a dual API: Datum-based for backend code and void*-based for frontend code, determined by the FRONTEND compilation flag
- Nodes are stored in a complete binary tree layout: root at index 0, left child of node i at 2*i+1, right child at 2*i+2, parent of node i at (i-1)/2
- The comparison function determines heap type: for max-heap, return <0 if a<b, 0 if a==b, >0 if a>b; for min-heap, conditions are reversed
- The bh_has_heap_property flag is used for debugging to detect when unordered operations have been performed and a heap rebuild is needed
- Memory allocation includes space for the structure plus the node array in a single block for efficiency
- Commonly used in PostgreSQL for merge operations, parallel query coordination, buffer management, and logical replication ordering