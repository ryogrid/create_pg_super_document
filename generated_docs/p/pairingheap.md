# pairingheap

## Location
[src/include/lib/pairingheap.h:71-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/pairingheap.h#L71-L76)

## Overview
A pairing heap data structure that provides an efficient priority queue implementation with O(log n) amortized complexity for most operations.

## Definition
```c
typedef struct pairingheap
{
    pairingheap_comparator ph_compare;    /* comparison function */
    void           *ph_arg;               /* opaque argument to ph_compare */
    pairingheap_node *ph_root;           /* current root of the heap */
} pairingheap;
```

## Detailed Description
The `pairingheap` structure implements a pairing heap, which is a type of heap data structure with excellent practical performance. It supports the standard heap operations: insertion, finding the minimum/maximum element, and deletion of the minimum/maximum element.

The pairing heap is particularly well-suited for applications that perform many insertions and minimum extractions, as these operations have very good amortized time complexity. The structure uses a comparison function pointer to determine the heap ordering, making it generic and reusable for different data types.

PostgreSQL uses pairing heaps extensively for priority queues in various subsystems including index scanning, query execution, and replication logic. The heap can be configured as either a min-heap or max-heap depending on the comparison function provided.

## Parameters / Member Variables
- `ph_compare`: Function pointer to a comparator that defines the heap ordering; returns <0, 0, or >0 for less than, equal, or greater than comparisons respectively
- `ph_arg`: Opaque pointer passed as the third argument to the comparison function, allowing context-specific comparison logic
- `ph_root`: Pointer to the root node of the heap tree, or NULL if the heap is empty

## Dependencies
- Functions called/Symbols referenced:
  - [pairingheap_node](pairingheap_node.md) (for the tree structure)
  - `pairingheap_comparator` (function pointer type)
- Called from (representative examples):
  - `pairingheap_allocate` (for creating new heaps)
  - `pairingheap_add` (for inserting elements)
  - `pairingheap_remove_first` (for extracting min/max)
  - `pairingheap_first` (for peeking at min/max)
  - [GISTScanOpaqueData](../G/GISTScanOpaqueData.md) (in GIST index scans)
  - `SpGistScanOpaqueData` (in SP-GIST index scans)
  - [IndexScanState](../I/IndexScanState.md) (in query execution)

## Notes and Other Information
- Can be allocated using `pairingheap_allocate()` or embedded directly in larger structures for memory efficiency
- Provides several convenience macros: `pairingheap_reset()`, `pairingheap_is_empty()`, and `pairingheap_is_singular()`
- The heap property (min or max) is determined entirely by the comparison function - the same structure can implement either
- Supports efficient merging of two heaps, which is the fundamental operation underlying the pairing heap algorithm
- Debug functionality is available when compiled with `PAIRINGHEAP_DEBUG` defined
- Thread safety is not provided - external synchronization is required for concurrent access
- Memory management of individual nodes is the responsibility of the caller, not the heap structure itself