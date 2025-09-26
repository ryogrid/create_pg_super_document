# ExprContext_CB

## Location
[src/include/nodes/execnodes.h:221-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L221-L226)

## Overview
ExprContext_CB is a callback node structure that implements a linked list of callback functions to be executed when an ExprContext is shut down.

## Definition

```c
typedef struct ExprContext_CB
{
	struct ExprContext_CB *next;
	ExprContextCallbackFunction function;
	Datum		arg;
} ExprContext_CB;
```
## Detailed Description
ExprContext_CB forms part of a callback system that allows registering cleanup functions to be executed when an expression context is destroyed. The structure implements a simple linked list where each node contains a function pointer and an argument to pass to that function. This mechanism is particularly useful for cleaning up resources allocated during expression evaluation, such as temporary memory, file handles, or other resources that need explicit cleanup when the expression evaluation context terminates.

## Parameters / Member Variables
- `*next`: Pointer to the next callback node in the linked list, NULL for the last node
- `function`: Function pointer of type ExprContextCallbackFunction to be called during cleanup
- `arg`: Datum argument to be passed to the callback function when invoked
## Dependencies
- Functions called/Symbols referenced:
  - ExprContextCallbackFunction (callback function type definition)
- Called from (representative examples):
  - [RegisterExprContextCallback](../R/RegisterExprContextCallback.md) (registers a new callback)
  - [UnregisterExprContextCallback](../U/UnregisterExprContextCallback.md) (removes a callback from the list)
  - [ShutdownExprContext](../S/ShutdownExprContext.md) (executes all registered callbacks during context cleanup)

## Notes and Other Information
- Callbacks are executed in the order they were registered (FIFO - First In, First Out)
- The callback system provides a clean way to ensure resource cleanup without requiring explicit cleanup calls from expression evaluation code
- Commonly used for cleaning up resources allocated by functions that store temporary data during expression evaluation
- The Datum argument allows passing context-specific data to the cleanup function