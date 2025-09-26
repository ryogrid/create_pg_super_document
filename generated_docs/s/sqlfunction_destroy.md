# sqlfunction_destroy

## Location
[src/backend/executor/functions.c:2123-2126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L2123-L2126)

## Overview
Destroys and deallocates memory for SQL function destination receiver objects when they are no longer needed.

## Definition
```c
static void sqlfunction_destroy(DestReceiver *self)
```

## Detailed Description
This function handles the destruction and cleanup of SQL function destination receiver objects. It serves as the destroy callback in the DestReceiver interface, responsible for releasing the memory allocated for the receiver when it's no longer needed.

The implementation is straightforward: it simply calls pfree() to deallocate the memory pointed to by the self parameter. This suggests that SQL function destination receivers don't maintain complex internal state that requires elaborate cleanup procedures - the simple memory deallocation is sufficient.

Unlike the startup and shutdown callbacks which are no-ops, this destroy function performs actual work by freeing memory, which is essential for preventing memory leaks in long-running database sessions.

## Parameters / Member Variables
- `self`: Pointer to the DestReceiver structure (specifically a DR_sqlfunction) to be destroyed and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL's memory deallocation function)
  - [DestReceiver](../D/DestReceiver.md) (parameter type)
- Called from (representative examples):
  - [CreateSQLFunctionDestReceiver](../C/CreateSQLFunctionDestReceiver.md) (sets this as destroy callback)
  - Used within SQLFunctionCachePtr context

## Notes and Other Information
- This is the only callback among the four SQL function DestReceiver callbacks that performs actual work
- The simple pfree() call indicates that DR_sqlfunction structures don't have complex nested allocations
- Essential for memory management in SQL function execution to prevent leaks
- Part of the standard DestReceiver lifecycle: create → startup → receive (multiple) → shutdown → destroy
- Located in src/backend/executor/functions.c with other SQL function execution infrastructure
- Static function scope indicates it's only used within the functions.c module