# MemoryContextDeleteOnly

## Location
[src/backend/utils/mmgr/mcxt.c:496-538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L496-L538)

## Overview
A subroutine of MemoryContextDelete that deletes a context with no children, handling deallocation, callback execution, and safe unlinking from the parent.

## Definition

```c
static void
MemoryContextDeleteOnly(MemoryContext context)
```
## Detailed Description
MemoryContextDeleteOnly is a specialized static function designed to safely delete a single memory context that has no children. As a subroutine of MemoryContextDelete, it handles the intricate details of context destruction while maintaining system stability and preventing memory corruption.

The function follows a carefully ordered sequence: first executing reset callbacks, then delinking the context from its parent, clearing the identity pointer, and finally calling the context-specific deletion method. This ordering is deliberate - executing callbacks before delinking ensures that if a callback fails, the context remains properly linked rather than becoming a dangling reference.

The function includes multiple safety assertions to prevent deletion of critical contexts like TopMemoryContext and CurrentMemoryContext, and verifies that the context has no children before proceeding.

## Parameters / Member Variables
- `context`: The memory context to delete. Must be a valid MemoryContext with no children.
## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - [MemoryContextCallResetCallbacks](MemoryContextCallResetCallbacks.md)
  - [MemoryContextSetParent](MemoryContextSetParent.md)
  - ident (context member)
  - VALGRIND_DESTROY_MEMPOOL
- Called from (representative examples):
  - [MemoryContextDelete](MemoryContextDelete.md)

## Notes and Other Information
- This is a static function, only accessible within the mcxt.c compilation unit
- Multiple safety assertions prevent deletion of critical system contexts and contexts with children
- Callbacks are executed before unlinking to prevent dangling references in case of callback failures
- The context's ident pointer is cleared to prevent potential dangling pointer issues
- Uses VALGRIND_DESTROY_MEMPOOL for memory debugging support
- The function prioritizes system stability by preferring memory leaks over crashes when errors occur
- Calls the context-specific delete_context method through the methods table for proper cleanup

## Simplified Source

```c
// Simplified version of MemoryContextDeleteOnly
static void MemoryContextDeleteOnly(MemoryContext context) {
    // Safety checks: verify context is valid and safe to delete
    Assert(MemoryContextIsValid(context));
    Assert(context != TopMemoryContext);
    Assert(context != CurrentMemoryContext);
    Assert(context->firstchild == NULL);

    // Step 1: Execute reset callbacks before unlinking
    // (prevents dangling references if callbacks fail)
    MemoryContextCallResetCallbacks(context);

    // Step 2: Safely delink from parent context
    MemoryContextSetParent(context, NULL);

    // Step 3: Clear identity pointer to prevent dangling references
    context->ident = NULL;

    // Step 4: Call context-specific deletion method
    context->methods->delete_context(context);

    // Step 5: Clean up memory debugging info
    VALGRIND_DESTROY_MEMPOOL(context);
}
```

Key simplifications made:
- Consolidated verbose comments into concise step-by-step descriptions
- Removed detailed explanatory comments while preserving the logic flow
- Maintained all safety assertions as they are critical for correctness
- Preserved the careful ordering of operations which is essential for stability
- Kept all function calls as they represent the core functionality