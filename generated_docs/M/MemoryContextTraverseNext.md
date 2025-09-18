# MemoryContextTraverseNext

## Location
src/backend/utils/mmgr/mcxt.c: 257 - 285

## Overview
A helper function that provides non-recursive traversal of memory context hierarchies, visiting all descendants of a given context in pre-order without risking stack overflow.

## Definition
```c
static MemoryContext MemoryContextTraverseNext(MemoryContext curr, MemoryContext top)
```

## Detailed Description
This function implements a non-recursive tree traversal algorithm for memory context hierarchies. It avoids recursion to prevent stack overflow issues that could occur with deep context hierarchies, which would be particularly problematic in error cleanup code paths. The function implements a pre-order traversal where a node is visited before its children. It follows a specific algorithm: first try to visit the first child, then move to the next sibling, and if no sibling exists, traverse back up to the parent and repeat until finding a sibling or reaching the top context.

## Parameters / Member Variables
- `curr`: The current MemoryContext node being processed in the traversal
- `top`: The root MemoryContext that defines the boundary of the traversal (traversal stops when returning to this context)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContext struct fields (firstchild, nextchild, parent)
- Called from (representative examples):
  - MemoryContextResetChildren
  - MemoryContextMemAllocated
  - MemoryContextMemConsumed
  - MemoryContextStatsInternal
  - MemoryContextCheck

## Notes and Other Information
- This is a static function used internally within the memory context system
- The function is designed specifically to avoid recursion and prevent stack overflow
- Returns NULL when the traversal is complete (when curr reaches the top context)
- Pre-order traversal means parent contexts are processed before their children
- Typical usage pattern is in a loop: process the initial context, then iterate through all descendants using this function
- Essential for operations that need to visit all contexts in a hierarchy safely, such as memory statistics gathering and context validation
- The traversal algorithm ensures every descendant context is visited exactly once