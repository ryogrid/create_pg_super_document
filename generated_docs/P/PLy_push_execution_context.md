# PLy_push_execution_context

## Location
[src/pl/plpython/plpy_main.c:391-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L391-L406)

## Overview
Creates and pushes a new execution context onto the PL/Python execution context stack for managing procedure state and memory allocation.

## Definition
static PLyExecutionContext *PLy_push_execution_context(bool atomic_context)

## Detailed Description
PLy_push_execution_context allocates and initializes a new execution context for PL/Python procedure calls, then pushes it onto the global execution context stack. The function selects an appropriate memory context based on whether the operation is atomic - using TopTransactionContext for atomic contexts (which survive longer) or PortalContext for non-atomic contexts. The new context is initialized with null values and linked to form a stack structure via the next pointer.

## Parameters / Member Variables
- atomic_context: Boolean flag determining memory context selection - true uses TopTransactionContext, false uses PortalContext

## Dependencies
- Functions called/Symbols referenced:
  - [PLyExecutionContext](PLyExecutionContext.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - TopTransactionContext
  - PortalContext
- Called from (representative examples):
  - [plpython3_call_handler](../p/plpython3_call_handler.md)
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md)

## Notes and Other Information
- This is a static function only accessible within the plpy_main.c module
- Memory context selection follows SPI patterns for consistency with PostgreSQL conventions
- The context stack allows for nested Python function calls to maintain separate execution states
- Each context maintains its own current procedure pointer and scratch context
- Must be paired with PLy_pop_execution_context to prevent memory leaks and maintain stack integrity

## Simplified Source

```c
static PLyExecutionContext *
PLy_push_execution_context(bool atomic_context)
{
    PLyExecutionContext *context;

    // Choose memory context based on atomicity (similar to SPI pattern)
    context = (PLyExecutionContext *)
        MemoryContextAlloc(atomic_context ? TopTransactionContext : PortalContext,
                          sizeof(PLyExecutionContext));

    // Initialize new context
    context->curr_proc = NULL;
    context->scratch_ctx = NULL;

    // Push onto execution context stack
    context->next = PLy_execution_contexts;
    PLy_execution_contexts = context;

    return context;
}
```