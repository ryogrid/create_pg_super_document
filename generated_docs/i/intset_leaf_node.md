# intset_leaf_node

## Location
[src/backend/lib/integerset.c:169-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L169-L186)

## Overview
A leaf node structure in PostgreSQL's IntegerSet B-tree that stores compressed integer sequences using Simple-8b encoding and maintains sibling linkage for efficient range scanning.

## Definition


## Detailed Description
The intset_leaf_node structure represents the leaf nodes in PostgreSQL's IntegerSet B-tree, where the actual integer values are stored in compressed form. Unlike internal nodes that guide searches, leaf nodes contain the compressed integer data using Simple-8b encoding to minimize memory usage.

Each leaf node stores up to MAX_LEAF_ITEMS (64) leaf_item structures, where each leaf_item contains a base value ('first') and a Simple-8b encoded codeword representing the differences from that base value. This compression scheme is particularly effective for storing ranges of consecutive or nearly-consecutive integers, which are common in PostgreSQL's usage patterns for tracking tuple IDs, block numbers, and other sequential identifiers.

The nodes are linked together via the 'next' pointer to form a singly-linked list of leaf nodes in sorted order, enabling efficient sequential scans and range queries without requiring tree traversal. This horizontal linkage is essential for operations that need to iterate over ranges of integers in the set.

## Parameters / Member Variables
- : Tree level of this node (always 0 for leaf nodes)
- : Current number of leaf_item structures stored in this node (0 to MAX_LEAF_ITEMS)
- : Pointer to the next leaf node in sorted order, null for the rightmost leaf
- : Array of leaf_item structures, each containing a compressed sequence of integers

## Dependencies
- Functions called/Symbols referenced:
  - [intset_leaf_node](intset_leaf_node.md) (self-reference for next pointer)
  - MAX_LEAF_ITEMS (capacity constant, value 64)  
  - [leaf_item](../l/leaf_item.md) (structure for compressed integer sequences)
- Referenced by:
  - [IntegerSet](../I/IntegerSet.md) (maintains pointers to leaf nodes)
  - [intset_new_leaf_node](intset_new_leaf_node.md)
  - [intset_flush_buffered_values](intset_flush_buffered_values.md)
  - [intset_update_upper](intset_update_upper.md)
  - [intset_is_member](intset_is_member.md)

## Notes and Other Information
Like intset_internal_node, this structure uses C-style inheritance by embedding the intset_node header, allowing polymorphic access. The leaf_item array stores compressed integer sequences where each item can hold up to 1 + SIMPLE8B_MAX_VALUES_PER_CODEWORD integers. The horizontal linkage through the 'next' pointer enables efficient range scans without requiring repeated tree traversals from the root. The Simple-8b compression provides significant space savings for typical PostgreSQL integer sequences while maintaining fast decompression performance.