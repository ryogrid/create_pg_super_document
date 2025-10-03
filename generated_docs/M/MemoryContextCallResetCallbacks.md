# MemoryContextCallResetCallbacks

## Location
[src/backend/utils/mmgr/mcxt.c:585-611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L585-L611)

## Overview
Internal function that executes all registered reset callbacks for a memory context before the context is reset or deleted.

## Definition

```c
static void
MemoryContextCallResetCallbacks(MemoryContext context)
```
## Detailed Description
This function is responsible for calling all registered reset callbacks associated with a memory context. It implements a safe callback execution pattern by removing each callback from the context's callback list before executing it. This ensures that if an error occurs during callback execution, the callback won't be called again during subsequent context reset or deletion operations.

The function processes callbacks in a LIFO (Last In, First Out) order by iterating through the linked list of callbacks stored in the context's  field. Each callback is removed from the list before being invoked with its associated argument.

## Parameters / Member Variables
- `context`: The memory context whose reset callbacks should be executed
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextCallback](MemoryContextCallback.md) (callback structure type)
- Called from (representative examples):
  - [MemoryContextResetOnly](MemoryContextResetOnly.md)
  - [MemoryContextDeleteOnly](MemoryContextDeleteOnly.md)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Callbacks are executed in reverse order of registration (LIFO)
- Each callback is removed from the list before execution to prevent double-execution in error scenarios
- The function handles the case where callbacks might trigger errors by ensuring safe cleanup
- Located in src/backend/utils/mmgr/mcxt.c:585-611

## Simplified Source

```c
// Simplified version of MemoryContextCallResetCallbacks
static void MemoryContextCallResetCallbacks(MemoryContext context) {
    MemoryContextCallback *cb;

    // Process all callbacks in LIFO order
    // Remove each callback before calling to prevent double-execution on errors
    while ((cb = context->reset_cbs) != NULL) {
        // Remove callback from list
        context->reset_cbs = cb->next;

        // Execute the callback function
        cb->func(cb->arg);
    }
}
```

Key simplifications made:
- Condensed the original comment into clearer inline comments
- Preserved the critical safety pattern of removing callbacks before execution
- Maintained the LIFO processing order through the linked list
- Kept the essential error-safe callback execution logic