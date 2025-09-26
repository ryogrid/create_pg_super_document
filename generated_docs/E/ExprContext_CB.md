# ExprContext_CB

## Location
src/include/nodes/execnodes.h: 221 - 226

## Overview
ExprContext_CB is a callback node structure that implements a linked list of callback functions to be executed when an ExprContext is shut down.

## Definition


## Detailed Description
ExprContext_CB forms part of a callback system that allows registering cleanup functions to be executed when an expression context is destroyed. The structure implements a simple linked list where each node contains a function pointer and an argument to pass to that function. This mechanism is particularly useful for cleaning up resources allocated during expression evaluation, such as temporary memory, file handles, or other resources that need explicit cleanup when the expression evaluation context terminates.

## Parameters / Member Variables
- : Pointer to the next callback node in the linked list, NULL for the last node
- : Function pointer of type ExprContextCallbackFunction to be called during cleanup
- : Datum argument to be passed to the callback function when invoked

## Dependencies
- Functions called/Symbols referenced:
  - ExprContextCallbackFunction (callback function type definition)
- Called from (representative examples):
  - RegisterExprContextCallback (registers a new callback)
  - UnregisterExprContextCallback (removes a callback from the list)
  - ShutdownExprContext (executes all registered callbacks during context cleanup)

## Notes and Other Information
- Callbacks are executed in the order they were registered (FIFO - First In, First Out)
- The callback system provides a clean way to ensure resource cleanup without requiring explicit cleanup calls from expression evaluation code
- Commonly used for cleaning up resources allocated by functions that store temporary data during expression evaluation
- The Datum argument allows passing context-specific data to the cleanup function