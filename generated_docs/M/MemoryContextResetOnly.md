# MemoryContextResetOnly

## Location
[src/backend/utils/mmgr/mcxt.c:402-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L402-L432)

## Overview
MemoryContextResetOnly releases all allocated space within a memory context without affecting its descendant contexts, providing focused memory cleanup for the specific context only.

## Definition
```c
void MemoryContextResetOnly(MemoryContext context)
```

## Detailed Description
This function performs a targeted reset of a single memory context, freeing all allocated memory within that context while leaving child contexts untouched. The function includes several important aspects:

1. **Performance Optimization**: Checks context->isReset to avoid redundant work if the context is already in a reset state
2. **Reset Callbacks**: Calls MemoryContextCallResetCallbacks to allow registered callbacks to perform cleanup before the actual reset
3. **Method Delegation**: Uses the context's specific reset method (context->methods->reset) to perform the actual memory deallocation
4. **State Management**: Sets context->isReset = true to mark the context as reset
5. **Valgrind Integration**: Includes Valgrind mempool destruction and recreation for memory debugging support

The function includes a notable design consideration regarding context->ident: it may become a dangling pointer if it points into the context's own memory, but the function doesn't set it to NULL to avoid breaking valid coding patterns where the ident is stored elsewhere.

## Parameters / Member Variables
- `context`: The MemoryContext to reset - must be a valid memory context

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid (to validate the context parameter)
  - MemoryContextCallResetCallbacks (to execute registered reset callbacks)
  - context->methods->reset (context-specific reset implementation)
  - VALGRIND_DESTROY_MEMPOOL, VALGRIND_CREATE_MEMPOOL (Valgrind debugging support)
- Called from (representative examples):
  - [MemoryContextReset](MemoryContextReset.md) (as part of comprehensive context reset)
  - [MemoryContextResetChildren](MemoryContextResetChildren.md) (when resetting child contexts)
  - [AllocSetDelete](../A/AllocSetDelete.md) (during context deletion)
  - [JsonTableResetRowPattern](../J/JsonTableResetRowPattern.md) (JSON table function operations)
  - mergeruns (during tuplesort operations)

## Notes and Other Information
- Unlike MemoryContextReset, this function does NOT delete child contexts
- The context->isReset flag prevents redundant reset operations for performance
- Reset callbacks allow custom cleanup logic before memory deallocation
- The function preserves the context structure and its relationships for continued use
- Valgrind integration helps detect memory leaks and corruption during development
- The ident pointer issue is a known design trade-off favoring flexibility over safety
- This is a lower-level function typically called by higher-level memory management routines
- Essential for implementing different reset strategies (with or without child deletion)
- The context remains valid and ready for new allocations after reset