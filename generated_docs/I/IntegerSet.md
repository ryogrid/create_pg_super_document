# IntegerSet

## Location
[src/backend/lib/integerset.c:197-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L197-L283)

## Overview
The main data structure representing a compressed set of 64-bit integers in PostgreSQL, implemented as an in-memory B-tree with buffering support and iterator functionality for efficient storage and retrieval of large integer collections.

## Definition


## Detailed Description
The IntegerSet structure is PostgreSQL's primary data structure for efficiently storing and managing large collections of 64-bit integers. It combines an in-memory B-tree with Simple-8b compression to minimize memory usage while maintaining fast lookup and insertion performance.

The structure employs a multi-layered approach: newly inserted integers are first collected in a buffer array (buffered_values), which reduces the overhead of individual B-tree insertions. When the buffer reaches capacity or when explicitly flushed, values are compressed using Simple-8b encoding and inserted into the appropriate leaf nodes of the B-tree.

The B-tree maintains up to MAX_TREE_LEVELS (11) levels and uses rightmost_nodes array to track the rightmost node at each level, enabling efficient append-only insertion patterns common in PostgreSQL. The leftmost_leaf pointer provides direct access for sequential iteration without tree traversal.

Iterator functionality allows efficient sequential access to all integers in the set, seamlessly transitioning from compressed B-tree values to buffered uncompressed values. The iterator maintains state to decode Simple-8b compressed leaf items on-demand, minimizing memory overhead during iteration.

## Parameters / Member Variables
- : Memory context for managing all allocations related to this IntegerSet
- : Total memory consumed by this IntegerSet (tracked for external monitoring)
- : Total count of distinct integers stored in the set
- : Maximum integer value present in the set (optimization for append-only workloads)
- : Current height/depth of the B-tree structure
- : Pointer to the root node of the B-tree
- : Array of pointers to rightmost nodes at each tree level (for efficient insertion)
- : Direct pointer to leftmost leaf node (for efficient iteration start)
- : Array holding recently inserted values before B-tree integration  
- : Count of values currently in the buffer array
- : Flag indicating whether iteration is currently in progress
- : Pointer to current batch of values being iterated over
- : Number of values in the current iteration batch
- : Index of next value to return in current batch
- : Current leaf node being processed during iteration
- : Index of next item to decode within current leaf node
- : Buffer for decoded values from compressed leaf items

## Dependencies
- Functions called/Symbols referenced:
  - [intset_node](../i/intset_node.md), intset_leaf_node (tree node structures)
  - MAX_TREE_LEVELS (11), MAX_BUFFERED_VALUES, MAX_VALUES_PER_LEAF_ITEM (capacity constants)
  - [simple8b_encode](../s/simple8b_encode.md), simple8b_decode, simple8b_contains (compression functions)
  - [intset_binsrch_uint64](../i/intset_binsrch_uint64.md), intset_binsrch_leaf (search functions)
- Referenced by:
  - [intset_create](../i/intset_create.md)
  - [intset_add_member](../i/intset_add_member.md), intset_is_member
  - [intset_begin_iterate](../i/intset_begin_iterate.md), intset_iterate_next
  - [intset_num_entries](../i/intset_num_entries.md), intset_memory_usage
  - Various test functions

## Notes and Other Information
The IntegerSet is designed for append-mostly workloads where integers are typically added in ascending order, making it highly suitable for PostgreSQL's needs such as tracking block numbers, tuple IDs, or transaction IDs. The combination of buffering and B-tree storage provides both fast insertion performance and memory efficiency. The requirement that new values must be added at the end simplifies tree maintenance but restricts usage to monotonic insertion patterns. Memory usage is carefully tracked to support PostgreSQL's memory management systems, and the iterator design enables efficient processing of very large integer sets without loading all values into memory simultaneously.