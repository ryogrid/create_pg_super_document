# MemoryContextResetChildren

## Location
[src/backend/utils/mmgr/mcxt.c:433-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L433-L453)

## Overview
Releases all space allocated within a context's descendants, but doesn't delete the contexts themselves, leaving the named context itself untouched.

## Definition

```c
void
MemoryContextResetChildren(MemoryContext context)
```
## Detailed Description
MemoryContextResetChildren is a memory management function that performs a selective reset operation on a memory context hierarchy. It traverses all descendant contexts of the specified context and resets each one, freeing all memory allocated within them while preserving the context structures themselves. This operation is useful when you want to clear memory from child contexts but maintain the context hierarchy for future use.

The function uses a depth-first traversal approach, visiting each descendant context exactly once and calling MemoryContextResetOnly() on each. The parent context specified in the parameter remains completely untouched.

## Parameters / Member Variables
- `context`: The parent memory context whose children will be reset. Must be a valid MemoryContext.
## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - [MemoryContextTraverseNext](MemoryContextTraverseNext.md)
  - [MemoryContextResetOnly](MemoryContextResetOnly.md)
- Called from (representative examples):
  - AllocHugeSizeIsValid

## Notes and Other Information
- The function includes an assertion to validate that the input context is valid before proceeding
- This operation is non-destructive to the context hierarchy structure - only the allocated memory within child contexts is freed
- The traversal uses MemoryContextTraverseNext() which ensures proper depth-first iteration through the context tree
- This function is particularly useful in scenarios where you need to clear temporary allocations in child contexts while preserving the context structure for reuse

## Simplified Source

```c
void
MemoryContextResetChildren(MemoryContext context)
{
    Assert(MemoryContextIsValid(context));

    // Traverse all child contexts and reset each one
    for (MemoryContext curr = context->firstchild;
         curr != NULL;
         curr = MemoryContextTraverseNext(curr, context))
    {
        MemoryContextResetOnly(curr);
    }
}
```