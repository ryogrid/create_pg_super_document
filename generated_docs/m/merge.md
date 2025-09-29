# merge

## Location
[src/backend/lib/pairingheap.c:79-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/pairingheap.c#L79-L111)

## Overview
A static helper function that merges two pairing heap subtrees into a single subtree while maintaining the heap property.

## Definition
```c
static pairingheap_node *merge(pairingheap *heap, pairingheap_node *a, pairingheap_node *b)
```

## Detailed Description
The `merge` function combines two pairing heap nodes (subtrees) into a single subtree by comparing their values using the heap's comparison function. The node with the larger value (according to the comparator) becomes the parent, and the node with the smaller value becomes its leftmost child. This operation is fundamental to the pairing heap data structure and maintains the heap property after the merge.

The function handles edge cases where one or both input nodes are NULL and ensures that the resulting structure maintains proper parent-child relationships. The next_sibling and prev_or_parent pointers of the input nodes are ignored during the merge, and the returned node's sibling pointers are left in an undefined state ("garbage") as noted in the comments.

This is a core primitive operation used by other pairing heap functions like `pairingheap_add` and during the complex merge operations in `merge_children`.

## Parameters / Member Variables
- `heap`: Pointer to the pairing heap structure containing the comparison function and argument
- `a`: First pairing heap node to merge
- `b`: Second pairing heap node to merge

## Dependencies
- Functions called/Symbols referenced:
  - heap->ph_compare (comparison function stored in heap)
  - [pairingheap](../p/pairingheap.md) (structure type)
  - [pairingheap_node](../p/pairingheap_node.md) (node structure type)
- Called from (representative examples):
  - [_bt_load](../b/_bt_load.md) (B-tree loading operations)
  - [pairingheap_add](../p/pairingheap_add.md) (adding elements to heap)
  - [merge_children](merge_children.md) (complex merge operations during deletion)

## Notes and Other Information
- This is a static function, only accessible within the pairingheap.c file
- The function assumes a max-heap behavior (larger values become parents)
- Input node sibling pointers are ignored and output sibling pointers are undefined
- Handles NULL inputs gracefully by returning the non-NULL node
- The merged structure maintains the leftmost-child representation used by pairing heaps
- Critical for maintaining O(log n) amortized performance in pairing heap operations

## Simplified Source

```c
// Simplified version of merge - merges two pairing heap subtrees
static pairingheap_node *
merge(pairingheap *heap, pairingheap_node *a, pairingheap_node *b)
{
    // Handle NULL cases - return the non-NULL node
    if (a == NULL) return b;
    if (b == NULL) return a;

    // Ensure 'a' has the larger value (max-heap property)
    if (heap->ph_compare(a, b, heap->ph_arg) < 0) {
        // Swap nodes so 'a' becomes the larger one
        pairingheap_node *tmp = a;
        a = b;
        b = tmp;
    }

    // Make 'b' the leftmost child of 'a'
    b->prev_or_parent = a;
    b->next_sibling = a->first_child;
    if (a->first_child) {
        a->first_child->prev_or_parent = b;
    }
    a->first_child = b;

    return a;  // Return the root of merged subtree
}
```

Key simplifications made:
- Simplified variable swapping logic with clearer inline initialization
- Added descriptive comments explaining each major step
- Clarified the max-heap behavior in comments
- Condensed the child linking logic while preserving correctness
- Made the NULL handling more explicit and readable