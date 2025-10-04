# PLy_pop_execution_context

## Location
[src/pl/plpython/plpy_main.c:407-419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L407-L419)

## Overview
Removes and cleans up the current execution context from the PL/Python execution context stack, freeing associated memory resources.

## Definition
static void PLy_pop_execution_context(void)

## Detailed Description
PLy_pop_execution_context pops the top execution context from the global PL/Python execution context stack and performs complete cleanup. The function first validates that a context exists on the stack, then removes it from the linked list structure, deletes any associated scratch memory context if it was allocated, and finally frees the context structure itself. This ensures proper memory management and prevents resource leaks during PL/Python procedure execution.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [PLyExecutionContext](PLyExecutionContext.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - elog
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [plpython3_call_handler](../p/plpython3_call_handler.md)
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md)

## Notes and Other Information
- This is a static function only accessible within the plpy_main.c module
- Includes error checking to ensure a context exists before attempting to pop it
- Properly cleans up the lazily-allocated scratch context if it was created during execution
- Must be called to balance each PLy_push_execution_context call to maintain stack integrity
- Used in both normal completion and error cleanup paths to ensure proper resource management
- The function maintains the global PLy_execution_contexts stack by updating the head pointer

## Simplified Source

```c
static void PLy_pop_execution_context(void) {
    PLyExecutionContext *context = PLy_execution_contexts;

    // Ensure context exists before popping
    if (context == NULL)
        elog(ERROR, "no Python function is currently executing");

    // Remove from stack
    PLy_execution_contexts = context->next;

    // Clean up memory resources
    if (context->scratch_ctx)
        MemoryContextDelete(context->scratch_ctx);
    pfree(context);
}
```