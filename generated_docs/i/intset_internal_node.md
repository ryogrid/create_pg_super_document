# intset_internal_node

## Location
[src/backend/lib/integerset.c:146-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L146-L164)

## Overview
An internal node structure in PostgreSQL's IntegerSet B-tree implementation that stores key values and pointers to child nodes, enabling efficient tree navigation and range queries over integer sets.

## Definition

```c
struct intset_internal_node
{
	/* common header, must match intset_node */
	uint16		level;			/* >= 1 on internal nodes */
	uint16		num_items;

	/*
	 * 'values' is an array of key values, and 'downlinks' are pointers to
	 * lower-level nodes, corresponding to the key values.
	 */
	uint64		values[MAX_INTERNAL_ITEMS];
	intset_node *downlinks[MAX_INTERNAL_ITEMS];
};
```
## Detailed Description
The intset_internal_node structure represents internal (non-leaf) nodes in PostgreSQL's IntegerSet B-tree, serving as index nodes that guide searches down to the appropriate leaf nodes. This structure extends the common intset_node header with arrays for storing key values and child pointers.

Internal nodes maintain a sorted array of key values that act as separators, with each key corresponding to a pointer to a lower-level node (either another internal node or a leaf node). The B-tree property is maintained such that all values in the subtree pointed to by downlinks[i] are less than or equal to values[i]. This organization enables efficient binary search within nodes and logarithmic-time traversal to locate specific values or ranges.

With MAX_INTERNAL_ITEMS set to 64, each internal node can hold up to 64 key-pointer pairs, resulting in approximately 1 KB per node. This fanout provides a good balance between memory usage and tree height, ensuring efficient cache utilization while maintaining shallow tree depths for fast access times.

## Parameters / Member Variables
- : Tree level of this node (≥ 1 for internal nodes, increases toward root)
- : Current number of key-pointer pairs stored in this node
- : Sorted array of 64-bit unsigned integer keys that serve as separators for child subtrees
- : Array of pointers to child nodes (intset_internal_node or intset_leaf_node structures) corresponding to each key

## Dependencies
- Functions called/Symbols referenced:
  - MAX_INTERNAL_ITEMS (capacity constant, value 64)
  - [intset_node](intset_node.md) (base structure extended by this node type)
- Referenced by:
  - [intset_create](intset_create.md)
  - [intset_new_internal_node](intset_new_internal_node.md)
  - [intset_update_upper](intset_update_upper.md)
  - [intset_is_member](intset_is_member.md)

## Notes and Other Information
The structure employs C-style inheritance by embedding the intset_node header as its first members, allowing internal nodes to be treated polymorphically as generic intset_node pointers. The arrays are sized to MAX_INTERNAL_ITEMS (64) to balance memory efficiency with tree fanout. The B-tree invariant requires that values[i] represents the maximum value in the subtree rooted at downlinks[i], enabling efficient range queries and insertions.