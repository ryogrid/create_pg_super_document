# RT_NODE_48_IS_CHUNK_USED

## Location
[src/include/lib/radixtree.h:778-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L778-L783)

## Overview
A macro that expands to a function name for checking whether a specific chunk (key byte) has an associated child pointer in a node48 structure of the radix tree.

## Definition
```c
#define RT_NODE_48_IS_CHUNK_USED RT_MAKE_NAME(node_48_is_chunk_used)

static inline bool
RT_NODE_48_IS_CHUNK_USED(RT_NODE_48 * node, uint8 chunk)
{
	return node->slot_idxs[chunk] != RT_INVALID_SLOT_IDX;
}
```

## Detailed Description
RT_NODE_48_IS_CHUNK_USED is a macro that generates a function name for determining whether a particular chunk (key byte) has a valid child pointer stored in a node48 structure. 

In the radix tree implementation, a node48 is a specialized node type that uses a sparse representation to efficiently store up to 48 children. It employs a two-level indexing scheme:

1. A 256-element slot_idxs array indexed directly by chunk values (0-255)
2. A children array containing the actual child pointers

The slot_idxs array stores indices into the children array, or RT_INVALID_SLOT_IDX (0xFF) if no child exists for that chunk. This function checks whether a chunk has a valid child by comparing its slot index to RT_INVALID_SLOT_IDX.

This design allows node48 to efficiently handle sparse key distributions where only a subset of possible chunk values are actually used, while maintaining O(1) lookup time.

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE_48 structure to query
- `chunk`: The 8-bit chunk value (key byte) to check for existence

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - RT_INVALID_SLOT_IDX (constant value 0xFF indicating unused slot)
  - [RT_NODE_48](RT_NODE_48.md) (structure type for 48-way radix tree nodes)
- Called from (representative examples):
  - [RT_FREE_RECURSE](RT_FREE_RECURSE.md) (at src/include/lib/radixtree.h:2016)
  - [RT_NODE_ITERATE_NEXT](RT_NODE_ITERATE_NEXT.md) (at src/include/lib/radixtree.h:2171)
  - [RT_VERIFY_NODE](RT_VERIFY_NODE.md) (at src/include/lib/radixtree.h:2747)
  - Various dump and debug functions (at src/include/lib/radixtree.h:2872, 2890)

## Notes and Other Information
This function is part of the node48 implementation in PostgreSQL's radix tree. Node48 represents an intermediate fanout between node16 (16 children) and node256 (256 children), providing a memory-efficient way to store moderate numbers of children.

The function is essential for tree traversal, iteration, and maintenance operations. It's used during:
- Tree iteration to find the next valid child
- Memory cleanup to identify which children need to be freed
- Verification to ensure tree consistency
- Debugging and diagnostic output

The node48 structure uses this function as part of its sparse child management strategy, allowing it to efficiently skip over unused chunk values during iteration and other operations.