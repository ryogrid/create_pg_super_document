# binaryheap_add

## Location
[src/common/binaryheap.c:154-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L154-L176)

## Overview
A function that inserts a new element into a binary heap while maintaining the heap property in O(log n) time complexity.

## Definition
void binaryheap_add(binaryheap *heap, bh_node_type d)

## Detailed Description
This function implements the standard heap insertion algorithm. It adds the new element at the end of the heap array (maintaining the complete binary tree structure), then uses the sift_up operation to restore the heap property by comparing the new element with its ancestors and swapping as needed until the heap property is satisfied. Unlike binaryheap_add_unordered(), this function maintains heap validity throughout the operation, ensuring the heap can be used immediately after insertion without requiring a rebuild operation.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure to insert the element into
- `d`: The data element (bh_node_type) to be inserted into the heap

## Dependencies
- Functions called/Symbols referenced:
  - [binaryheap](binaryheap.md) (struct type)
  - bh_node_type (type definition)
  - FRONTEND (preprocessor macro for conditional compilation)
  - [pg_fatal](../p/pg_fatal.md) (frontend error function)
  - elog (backend error function)
  - [sift_up](../s/sift_up.md) (maintains heap property by moving nodes upward)
- Called from (representative examples):
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md) (in src/backend/postmaster/pgarch.c)
  - [move_to_ready_heap](../m/move_to_ready_heap.md) (in src/bin/pg_dump/pg_backup_archiver.c)
  - [reduce_dependencies](../r/reduce_dependencies.md) (in src/bin/pg_dump/pg_backup_archiver.c)
  - [TopoSort](../T/TopoSort.md) (in src/bin/pg_dump/pg_dump_sort.c)

## Notes and Other Information
- The function will terminate the program (pg_fatal in frontend, elog ERROR in backend) if the heap capacity is exceeded
- Maintains O(log n) time complexity by using the sift_up algorithm to restore heap property
- The heap remains valid and usable immediately after this operation, unlike binaryheap_add_unordered()
- Used for dynamic insertion of elements into an already-constructed heap during runtime
- The element is initially placed at the end of the array to preserve the complete binary tree structure, then moved to its proper position
- More expensive than binaryheap_add_unordered() but provides immediate heap validity
- Commonly used in priority queue operations where elements need to be inserted into an existing working heap

## Simplified Source

```c
void binaryheap_add(binaryheap *heap, bh_node_type d) {
    // Check for capacity overflow
    if (heap->bh_size >= heap->bh_space) {
#ifdef FRONTEND
        pg_fatal("out of binary heap slots");
#else
        elog(ERROR, "out of binary heap slots");
#endif
    }

    // Add new element at end of heap array
    heap->bh_nodes[heap->bh_size] = d;
    heap->bh_size++;

    // Restore heap property by sifting the new element up
    sift_up(heap, heap->bh_size - 1);
}
```