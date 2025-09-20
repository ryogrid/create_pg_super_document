# intset_node

## Location
[src/backend/lib/integerset.c:139-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L139-L145)

## Overview
A base structure that defines the common header fields for both internal and leaf nodes in PostgreSQL's IntegerSet B-tree implementation, providing fundamental node metadata for tree traversal and manipulation.

## Definition

```c
struct intset_node
{
	uint16		level;			/* tree level of this node */
	uint16		num_items;		/* number of items in this node */
};
```
## Detailed Description
The intset_node structure serves as the foundation for PostgreSQL's IntegerSet data structure, which implements a B-tree for efficiently storing and querying sets of integers. This structure defines the common header that appears at the beginning of both internal nodes (intset_internal_node) and leaf nodes (intset_leaf_node) in the tree hierarchy.

The structure uses a compact 4-byte header design to minimize memory overhead while providing essential metadata for tree operations. The level field enables efficient tree traversal by indicating the node's position in the tree hierarchy, with leaf nodes at level 0 and internal nodes at higher levels. The num_items field tracks the current occupancy of each node, which is crucial for determining when nodes need to be split during insertion operations or merged during deletion operations.

## Parameters / Member Variables
- : Tree level of this node (0 for leaf nodes, increasing for higher internal nodes)
- : Current number of items stored in this node, used for capacity management and tree balancing

## Dependencies
- Functions called/Symbols referenced: None (base structure)
- Referenced by:
  - [intset_internal_node](intset_internal_node.md) (extends this structure)
  - [IntegerSet](../I/IntegerSet.md) (contains pointers to nodes of this type)
  - [intset_flush_buffered_values](intset_flush_buffered_values.md)
  - [intset_update_upper](intset_update_upper.md)  
  - [intset_is_member](intset_is_member.md)

## Notes and Other Information
This structure employs inheritance-like composition in C, where both intset_internal_node and intset_leaf_node structures begin with these same fields, allowing for polymorphic access to common node properties. The 16-bit integer types are chosen to balance memory efficiency with practical limits on tree depth and node capacity in typical PostgreSQL workloads.