# IntegerSet

## Location
[src/backend/lib/integerset.c:197-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L197-L283)

## Overview
The main data structure representing a compressed set of 64-bit integers in PostgreSQL, implemented as an in-memory B-tree with buffering support and iterator functionality for efficient storage and retrieval of large integer collections.

## Definition

```c
struct IntegerSet
{
	/*
	 * 'context' is the memory context holding this integer set and all its
	 * tree nodes.
	 *
	 * 'mem_used' tracks the amount of memory used.  We don't do anything with
	 * it in integerset.c itself, but the callers can ask for it with
	 * intset_memory_usage().
	 */
	MemoryContext context;
	uint64		mem_used;

	uint64		num_entries;	/* total # of values in the set */
	uint64		highest_value;	/* highest value stored in this set */

	/*
	 * B-tree to hold the packed values.
	 *
	 * 'rightmost_nodes' hold pointers to the rightmost node on each level.
	 * rightmost_parent[0] is rightmost leaf, rightmost_parent[1] is its
	 * parent, and so forth, all the way up to the root. These are needed when
	 * adding new values. (Currently, we require that new values are added at
	 * the end.)
	 */
	int			num_levels;		/* height of the tree */
	intset_node *root;			/* root node */
	intset_node *rightmost_nodes[MAX_TREE_LEVELS];
	intset_leaf_node *leftmost_leaf;	/* leftmost leaf node */

	/*
	 * Holding area for new items that haven't been inserted to the tree yet.
	 */
	uint64		buffered_values[MAX_BUFFERED_VALUES];
	int			num_buffered_values;

	/*
	 * Iterator support.
	 *
	 * 'iter_values' is an array of integers ready to be returned to the
	 * caller; 'iter_num_values' is the length of that array, and
	 * 'iter_valueno' is the next index.  'iter_node' and 'iter_itemno' point
	 * to the leaf node, and item within the leaf node, to get the next batch
	 * of values from.
	 *
	 * Normally, 'iter_values' points to 'iter_values_buf', which holds items
	 * decoded from a leaf item.  But after we have scanned the whole B-tree,
	 * we iterate through all the unbuffered values, too, by pointing
	 * iter_values to 'buffered_values'.
	 */
	bool		iter_active;	/* is iteration in progress? */

	const uint64 *iter_values;
	int			iter_num_values;	/* number of elements in 'iter_values' */
	int			iter_valueno;	/* next index into 'iter_values' */

	intset_leaf_node *iter_node;	/* current leaf node */
	int			iter_itemno;	/* next item in 'iter_node' to decode */

	uint64		iter_values_buf[MAX_VALUES_PER_LEAF_ITEM];
};
```
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