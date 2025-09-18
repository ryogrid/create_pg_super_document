# RT_NODE_ITERATE_NEXT

## Location
[src/include/lib/radixtree.h:2122-2217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2122-L2217)

## Overview
RT_NODE_ITERATE_NEXT is a macro that expands to a static inline function that advances iteration within a single radix tree node and returns the next child pointer.

## Definition
```c
#define RT_NODE_ITERATE_NEXT RT_MAKE_NAME(node_iterate_next)
```

Function signature:
```c
static inline RT_PTR_ALLOC *RT_NODE_ITERATE_NEXT(RT_ITER * iter, int level);
```

## Detailed Description
RT_NODE_ITERATE_NEXT is a preprocessor macro that generates a function name for advancing iteration within a single node of the radix tree. This is an internal helper function used by the public iteration interface. The function handles the different node types (RT_NODE_KIND_4, RT_NODE_KIND_16, RT_NODE_KIND_48, RT_NODE_KIND_256) and returns the next child pointer at the specified level.

The function performs the following operations:
1. **Node Type Dispatch**: Uses a switch statement to handle different node types based on the node's kind field
2. **Node-Specific Iteration**: For each node type, implements appropriate iteration logic:
   - **Kind 4/16**: Direct array iteration using simple index increment
   - **Kind 48/256**: Sparse array iteration, searching for the next used chunk
3. **Boundary Checking**: Returns NULL when reaching the end of a node's children
4. **Key Construction**: Updates the iterator's key with the current key chunk at the appropriate bit position
5. **Index Management**: Advances the node iterator's index for the next call

The function is marked as static inline for performance, as it's called frequently during tree traversal.

## Parameters / Member Variables
- `iter`: Pointer to the RT_ITER structure containing iteration state
- `level`: The tree level (0 is leaf level) at which to perform the node iteration

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (for name generation)
  - [RT_NODE_48_IS_CHUNK_USED](RT_NODE_48_IS_CHUNK_USED.md) (for checking chunk usage in 48-node)
  - [RT_NODE_48_GET_CHILD](RT_NODE_48_GET_CHILD.md) (for getting child in 48-node)
  - [RT_NODE_256_IS_CHUNK_USED](RT_NODE_256_IS_CHUNK_USED.md) (for checking chunk usage in 256-node)
  - [RT_NODE_256_GET_CHILD](RT_NODE_256_GET_CHILD.md) (for getting child in 256-node)
- Called from (representative examples):
  - [RT_ITERATE_NEXT](RT_ITERATE_NEXT.md) (main iteration function)

## Notes and Other Information
- This is an internal static inline function, not part of the public API
- Handles all four radix tree node types with optimized iteration strategies
- Updates the iterator's key as it advances through the node
- Returns NULL when no more children are available at the current level
- Part of PostgreSQL's generic radix tree implementation located in src/include/lib/radixtree.h:241
- Performance-critical function that should remain inlined for efficiency