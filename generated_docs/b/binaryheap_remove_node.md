# binaryheap_remove_node

## Location
[src/common/binaryheap.c:225-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L225-L254)

## Overview
Removes the nth (zero-based) node from the heap and maintains the heap property through sifting operations.

## Definition
```c
void binaryheap_remove_node(binaryheap *heap, int n)
```

## Detailed Description
This function removes a specific node at index n from the binary heap, not just the root node. It maintains the heap property by comparing the replacement node with the removed node and deciding whether to sift up or down. The operation has O(log n) worst-case time complexity.

The function performs the following steps:
1. Validates that the heap is not empty, has the heap property, and n is a valid index
2. Compares the last node in the heap with the node being removed using the heaps comparison function
3. Decreases the heap size and places the last node in the vacated position
4. Based on the comparison result:
   - If replacement > removed: calls sift_up to move the replacement toward the root
   - If replacement < removed: calls sift_down to move the replacement toward the leaves
   - If equal: no sifting needed (heap property already maintained)

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure
- `n`: Zero-based index of the node to remove (must be >= 0 and < heap->bh_size)

## Dependencies
- Functions called/Symbols referenced:
  - binaryheap_empty (heap validation)
  - [sift_up](../s/sift_up.md) (heap rebalancing upward)
  - [sift_down](../s/sift_down.md) (heap rebalancing downward)
  - heap->bh_compare (comparison function for heap ordering)
- Called from (representative examples):
  - [pop_next_work_item](../p/pop_next_work_item.md) (bin/pg_dump/pg_backup_archiver.c:4593)

## Notes and Other Information
- The caller must ensure there are at least (n + 1) nodes in the heap
- More complex than binaryheap_remove_first because it can remove any node, not just the root
- The comparison function determines the direction of sifting needed
- Used in specialized scenarios where arbitrary node removal is required, such as in pg_dump work scheduling
- This is a destructive operation that modifies the heap structure

## Simplified Source

```c
void binaryheap_remove_node(binaryheap *heap, int n) {
    int cmp;

    // Validate heap state and bounds
    Assert(!binaryheap_empty(heap) && heap->bh_has_heap_property);
    Assert(n >= 0 && n < heap->bh_size);

    // Compare last node with the one being removed
    cmp = heap->bh_compare(heap->bh_nodes[--heap->bh_size],
                           heap->bh_nodes[n],
                           heap->bh_arg);

    // Move last node to vacated position
    heap->bh_nodes[n] = heap->bh_nodes[heap->bh_size];

    // Restore heap property by sifting in appropriate direction
    if (cmp > 0)
        sift_up(heap, n);        // Replacement is larger, move up
    else if (cmp < 0)
        sift_down(heap, n);      // Replacement is smaller, move down
    // If cmp == 0, no sifting needed
}
```