# RT_NODE_48

## Location
[src/include/lib/radixtree.h:542-561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L542-L561)

## Overview
RT_NODE_48 is a macro that generates a type name for a 48-slot adaptive radix tree node structure used in PostgreSQL's templated radix tree implementation.

## Definition

```c
typedef struct RT_NODE_48
{
	RT_NODE		base;

	/* bitmap to track which slots are in use */
	bitmapword	isset[RT_BM_IDX(RT_FANOUT_48_MAX)];

	/*
	 * Lookup table for indexes into the children[] array. We make this the
	 * last fixed-size member so that it's convenient to memset separately
	 * from the previous members.
	 */
	uint8		slot_idxs[RT_NODE_MAX_SLOTS];

/* Invalid index */
#define RT_INVALID_SLOT_IDX	0xFF

	/* number of children depends on size class */
	RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
}			RT_NODE_48;
```
## Detailed Description
RT_NODE_48 is part of PostgreSQL's adaptive radix tree (ART) implementation, which provides a memory-efficient and cache-friendly data structure for storing key-value pairs. This macro generates a prefixed type name for the 48-slot node variant, which is one of four node types in the adaptive radix tree hierarchy (4, 16, 48, and 256 slots).

The actual structure definition for the 48-slot node contains:
- A base RT_NODE header with common node metadata
- A bitmap (isset array) to track which slots are currently in use
- A slot lookup table (slot_idxs) that maps byte values (0-255) to indices in the children array

This node type is used when a node needs to store between 17-48 child pointers, representing an intermediate growth stage between the smaller 16-slot node and the larger 256-slot node. The 48-slot design uses indirection through the slot_idxs array to efficiently pack child pointers without wasting space on unused slots.

## Parameters / Member Variables
- : RT_NODE structure containing common node metadata (kind, count, fanout)
- : Bitmap array tracking which slots in the children array are occupied
- : Lookup table mapping byte values (0-255) to indices in the children array
- : Variable-length array of child pointers (allocated separately)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX
  - RT_PREFIX
- Called from (representative examples):
  - RT_FANOUT_48
  - [RT_NODE_48_IS_CHUNK_USED](RT_NODE_48_IS_CHUNK_USED.md)
  - [RT_NODE_48_GET_CHILD](RT_NODE_48_GET_CHILD.md)
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md)
  - [RT_NODE_SEARCH](RT_NODE_SEARCH.md)
  - [RT_GROW_NODE_48](RT_GROW_NODE_48.md)
  - [RT_ADD_CHILD_48](RT_ADD_CHILD_48.md)
  - [RT_GROW_NODE_16](RT_GROW_NODE_16.md)
  - [RT_FREE_RECURSE](RT_FREE_RECURSE.md)
  - [RT_NODE_ITERATE_NEXT](RT_NODE_ITERATE_NEXT.md)
  - [RT_SHRINK_NODE_256](RT_SHRINK_NODE_256.md)
  - [RT_SHRINK_NODE_48](RT_SHRINK_NODE_48.md)
  - [RT_REMOVE_CHILD_48](RT_REMOVE_CHILD_48.md)
  - [RT_VERIFY_NODE](RT_VERIFY_NODE.md)

## Notes and Other Information
The 48-slot node represents a key optimization in the adaptive radix tree design. Instead of allocating a full 256-slot array (which would waste significant memory when sparsely populated), it uses a two-level indirection system:

1. The slot_idxs array maps any possible byte value (0-255) to an index in the much smaller children array
2. The isset bitmap efficiently tracks which of the 48 slots are actually in use
3. This design provides O(1) lookup time while maintaining memory efficiency

The node automatically grows from a 16-slot node when it exceeds capacity and shrinks from a 256-slot node when it becomes sparse enough. This adaptive behavior is central to the ART algorithm's ability to balance memory usage with performance across different key distributions.