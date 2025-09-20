# RT_FREE_RECURSE

## Location
[src/include/lib/radixtree.h:1965-2060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1965-L2060)

## Overview
RT_FREE_RECURSE is a macro that generates a function name for recursively freeing all nodes in a radix tree structure, including memory allocated in the DSA (Dynamic Shared Area).

## Definition

```c
static void
RT_FREE_RECURSE(RT_RADIX_TREE * tree, RT_PTR_ALLOC ptr, int shift)
```
## Detailed Description
RT_FREE_RECURSE is part of PostgreSQL's generic radix tree implementation. This macro uses the RT_MAKE_NAME helper to generate a prefixed function name that recursively traverses and frees all nodes in a radix tree structure. The actual function signature generated would be:



This is an internal helper function (note the static modifier) that performs a depth-first traversal of the radix tree, freeing child nodes recursively before freeing parent nodes. It handles different node types (RT_NODE_KIND_4, RT_NODE_KIND_16, RT_NODE_KIND_48, RT_NODE_KIND_256) and properly manages memory allocated in the DSA area for shared memory trees. The function includes stack depth checking to prevent stack overflow during deep recursions.

## Parameters / Member Variables
- Uses RT_MAKE_NAME macro to construct the actual function name
- **tree**: Pointer to the radix tree structure containing the nodes to free
- **ptr**: Allocated pointer to the current node being processed
- **shift**: Current bit shift level in the radix tree traversal

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX
  - RT_PREFIX (defined by the including code)
  - check_stack_depth (PostgreSQL stack overflow protection)
  - [dsa_free](../d/dsa_free.md) (Dynamic Shared Area memory freeing)
  - [RT_PTR_SET_LOCAL](RT_PTR_SET_LOCAL.md) (converts DSA pointer to local pointer)
  - [RT_CHILDPTR_IS_VALUE](RT_CHILDPTR_IS_VALUE.md) (checks if child pointer is a value or node)
- Called from (representative examples):
  - [RT_FREE](RT_FREE.md) (main tree freeing function)
  - [RT_FREE_RECURSE](RT_FREE_RECURSE.md) (recursive self-calls during traversal)

## Notes and Other Information
- Internal helper function, not part of the public API
- Part of PostgreSQL's template-based radix tree implementation
- Performs depth-first traversal to ensure proper memory cleanup
- Handles different radix tree node kinds with varying child counts
- Includes stack depth checking to prevent deep recursion issues
- Essential for preventing memory leaks when freeing shared memory radix trees
- Only frees non-value child pointers to avoid freeing embedded values
- Used during tree destruction and cleanup operations