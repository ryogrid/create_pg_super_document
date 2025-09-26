# GetErrorContextStack

## Location
[src/backend/utils/error/elog.c:2056-2107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2056-L2107)

## Overview
GetErrorContextStack retrieves and formats the current error context stack for display and diagnostic purposes by traversing registered error context callbacks.

## Definition

```c
char *
GetErrorContextStack(void)
```
## Detailed Description
GetErrorContextStack collects context information from all registered error context callbacks and returns it as a formatted string. The function works by:

1. Setting up a temporary error stack entry for collecting context information
2. Configuring the associated memory context to be the caller's context
3. Traversing the error_context_stack and calling each callback function
4. Each callback is expected to call errcontext() to add context information
5. Cleaning up the temporary stack entry and returning the collected context string

The function ensures that all memory allocations are done in the caller's memory context, making it the caller's responsibility to free the returned string when done.

## Parameters / Member Variables
This function takes no parameters and returns a dynamically allocated string.

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (type)
  - [get_error_stack_entry](../g/get_error_stack_entry.md)
  - ErrorContextCallback callbacks (through callback field)
  - CurrentMemoryContext (global variable)
  - error_context_stack (global variable)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns a pstrdup'd string that must be freed by the caller
- Uses recursion_depth tracking to handle potential recursive calls
- Memory allocations are done in the caller's context for proper cleanup
- The function is designed to handle errors that may occur during callback execution
- Context callbacks are expected to call errcontext() to contribute to the context string
- The returned string contains the complete error call stack context information
- Used primarily for diagnostic and debugging purposes to understand error context