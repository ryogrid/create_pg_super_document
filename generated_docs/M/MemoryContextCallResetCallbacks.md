# MemoryContextCallResetCallbacks

## Location
src/backend/utils/mmgr/mcxt.c: 585 - 611

## Overview
Internal function that executes all registered reset callbacks for a memory context before the context is reset or deleted.

## Definition


## Detailed Description
This function is responsible for calling all registered reset callbacks associated with a memory context. It implements a safe callback execution pattern by removing each callback from the context's callback list before executing it. This ensures that if an error occurs during callback execution, the callback won't be called again during subsequent context reset or deletion operations.

The function processes callbacks in a LIFO (Last In, First Out) order by iterating through the linked list of callbacks stored in the context's  field. Each callback is removed from the list before being invoked with its associated argument.

## Parameters / Member Variables
- : The memory context whose reset callbacks should be executed

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextCallback (callback structure type)
- Called from (representative examples):
  - MemoryContextResetOnly
  - MemoryContextDeleteOnly

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Callbacks are executed in reverse order of registration (LIFO)
- Each callback is removed from the list before execution to prevent double-execution in error scenarios
- The function handles the case where callbacks might trigger errors by ensuring safe cleanup
- Located in src/backend/utils/mmgr/mcxt.c:585-611