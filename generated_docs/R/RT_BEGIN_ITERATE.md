# RT_BEGIN_ITERATE

## Location
[src/include/lib/radixtree.h:2094-2121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2094-L2121)

## Overview
RT_BEGIN_ITERATE is a macro that expands to a function name for creating and initializing an iterator to traverse all key-value pairs in a radix tree.

## Definition
```c
#define RT_BEGIN_ITERATE RT_MAKE_NAME(begin_iterate)
```

Function signature:
```c
RT_SCOPE RT_ITER *RT_BEGIN_ITERATE(RT_RADIX_TREE * tree);
```

## Detailed Description
RT_BEGIN_ITERATE is a preprocessor macro that generates a function name for creating an iterator for radix tree traversal. The function allocates and initializes an RT_ITER structure that maintains the state needed to iterate through all key-value pairs in the radix tree in ascending order of keys.

The function performs the following initialization steps:
1. Allocates zero-initialized memory for the iterator in the tree's iterator context
2. Stores a reference to the tree in the iterator
3. Sets up the root node as the starting point for iteration
4. Initializes the iteration level stack, setting the current level to the top level
5. Sets the initial index to 0 for starting the traversal

The iterator uses a stack-based approach to track the current position at each level of the radix tree, enabling proper depth-first traversal that produces keys in sorted order.

## Parameters / Member Variables
- `tree`: Pointer to the RT_RADIX_TREE structure to iterate over. Must be a valid, previously created radix tree.

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (for name generation)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (for iterator allocation)
  - [RT_PTR_SET_LOCAL](RT_PTR_SET_LOCAL.md) (for setting up root node pointer)
- Called from (representative examples):
  - User code that needs to traverse all tree entries
  - Database scan operations
  - Tree debugging and analysis functions

## Notes and Other Information
- Returns a newly allocated RT_ITER pointer that must be freed with RT_END_ITERATE
- The caller is responsible for proper locking in shared memory scenarios
- Iteration produces key-value pairs in ascending order of keys
- The iterator maintains a stack of RT_NODE_ITER structures for each tree level
- Part of PostgreSQL's generic radix tree implementation located in src/include/lib/radixtree.h:187
- Should be paired with RT_ITERATE_NEXT and RT_END_ITERATE for complete iteration

## Simplified Source

```c
// Macro that expands to: RT_PREFIX_begin_iterate
#define RT_BEGIN_ITERATE RT_MAKE_NAME(begin_iterate)

// Generated function (simplified logic):
RT_SCOPE RT_ITER *RT_BEGIN_ITERATE(RT_RADIX_TREE *tree) {
    // Allocate iterator in tree's iteration context
    RT_ITER *iter = MemoryContextAllocZero(tree->iter_context, sizeof(RT_ITER));

    // Initialize iterator with tree reference
    iter->tree = tree;

    // Set up root node as starting point
    if (tree->ctl->root != RT_INVALID_PTR_ALLOC) {
        iter->top_level = tree->ctl->start_shift / RT_SPAN;
        iter->node_iters[iter->top_level].node =
            RT_PTR_SET_LOCAL(tree, tree->ctl->root);
        iter->node_iters[iter->top_level].idx = 0;
        iter->cur_level = iter->top_level;
    } else {
        // Empty tree - set invalid state
        iter->cur_level = -1;
    }

    return iter;
}
```