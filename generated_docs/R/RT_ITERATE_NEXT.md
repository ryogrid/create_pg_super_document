# RT_ITERATE_NEXT

## Location
src/include/lib/radixtree.h: 2218 - 2267

## Overview
RT_ITERATE_NEXT is a macro that expands to a function name for advancing iteration through a radix tree and returning the next key-value pair.

## Definition
```c
#define RT_ITERATE_NEXT RT_MAKE_NAME(iterate_next)
```

Function signature:
```c
RT_SCOPE RT_VALUE_TYPE *RT_ITERATE_NEXT(RT_ITER * iter, uint64 *key_p);
```

## Detailed Description
RT_ITERATE_NEXT is a preprocessor macro that generates a function name for the main iteration function that traverses a radix tree and returns key-value pairs in ascending order of keys. This function implements a depth-first traversal using the iterator's level stack to maintain state across calls.

The function performs the following algorithm:
1. **Level-based Loop**: Continues iterating while the current level is within bounds
2. **Node Iteration**: Uses RT_NODE_ITERATE_NEXT to get the next child pointer at the current level
3. **Leaf Value Handling**: When at level 0 (leaf level) and a slot is found:
   - Sets the output key parameter
   - Returns the value (either embedded or via pointer)
4. **Tree Descent**: When a child slot is found at inner nodes:
   - Moves down one level
   - Initializes the new level's node iterator
5. **Tree Ascent**: When no more children are found at the current level:
   - Moves up one level to continue iteration
6. **Completion**: Returns NULL when all nodes have been visited

The function handles both embedded values (for small data) and pointer-based values, using RT_CHILDPTR_IS_VALUE to distinguish between them.

## Parameters / Member Variables
- `iter`: Pointer to the RT_ITER structure containing iteration state
- `key_p`: Pointer to uint64 where the current key will be stored when a value is found

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (for name generation)
  - [RT_NODE_ITERATE_NEXT](RT_NODE_ITERATE_NEXT.md) (for node-level iteration)
  - [RT_CHILDPTR_IS_VALUE](RT_CHILDPTR_IS_VALUE.md) (for checking if slot contains embedded value)
  - [RT_PTR_SET_LOCAL](RT_PTR_SET_LOCAL.md) (for setting up local pointers)
- Called from (representative examples):
  - User code performing complete tree traversal
  - Database scan operations
  - Tree analysis and debugging tools

## Notes and Other Information
- Returns key-value pairs in ascending order of keys
- Returns NULL when iteration is complete
- The caller must handle the returned key through the key_p parameter
- Works with both embedded and pointer-based values
- Maintains iteration state across calls using the iterator's level stack
- Part of PostgreSQL's generic radix tree implementation located in src/include/lib/radixtree.h:188
- Should be used between RT_BEGIN_ITERATE and RT_END_ITERATE calls