# binaryheap_allocate

## Location
src/common/binaryheap.c: 39 - 62

## Overview
Allocates and initializes a new binary heap data structure with the specified capacity and comparison function.

## Definition
```c
binaryheap *binaryheap_allocate(int capacity, binaryheap_comparator compare, void *arg)
```

## Detailed Description
The `binaryheap_allocate` function creates a new binary heap with the given capacity. It allocates memory for the heap structure including space for the specified number of nodes. The heap is initialized with a comparison function that defines the heap property (min-heap or max-heap behavior), and an optional argument that is passed to the comparison function. The newly created heap starts empty (size 0) and maintains the heap property flag set to true.

## Parameters / Member Variables
- `capacity`: Maximum number of nodes the heap can store
- `compare`: Comparison function that defines the heap ordering property
- `arg`: User-defined argument passed to the comparison function

## Dependencies
- Functions called/Symbols referenced:
  - offsetof (macro for calculating struct member offset)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - [binaryheap](binaryheap.md) (struct type)
  - bh_node_type (node data type)
  - binaryheap_comparator (function pointer type)
- Called from (representative examples):
  - [gather_merge_setup](../g/gather_merge_setup.md)
  - ExecInitMergeAppend
  - [PgArchiverMain](../P/PgArchiverMain.md)
  - [ReorderBufferIterTXNInit](../R/ReorderBufferIterTXNInit.md)
  - BufferSync
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)
  - [TopoSort](../T/TopoSort.md)

## Notes and Other Information
- The function calculates the total memory needed using offsetof to account for the variable-length array of nodes
- The heap is initialized in a valid empty state with heap property maintained
- Memory is allocated using PostgreSQL's palloc function which handles out-of-memory conditions
- The heap capacity is fixed at allocation time and cannot be changed later