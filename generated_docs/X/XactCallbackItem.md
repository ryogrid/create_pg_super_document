# XactCallbackItem

## Location
[src/backend/access/transam/xact.c:305-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L305-L310)

## Overview
XactCallbackItem is a linked list node structure that manages transaction callback functions, enabling modules to register for start-of-transaction and end-of-transaction notifications.

## Definition

```c
typedef struct XactCallbackItem
{
	struct XactCallbackItem *next;
	XactCallback callback;
	void	   *arg;
} XactCallbackItem;
```
## Detailed Description
XactCallbackItem implements a simple linked list to maintain transaction callback functions that need to be invoked at specific transaction lifecycle events. This mechanism allows various PostgreSQL subsystems and extensions to register cleanup functions, state management routines, or other operations that must be synchronized with transaction boundaries. The structure provides a lightweight and flexible way to extend transaction processing without modifying core transaction management code.

## Parameters / Member Variables
- : Pointer to the next callback item in the linked list
- : Function pointer to the callback function (XactCallback type)
- : Generic void pointer to user-defined argument data passed to the callback

## Dependencies
- Functions called/Symbols referenced:
  - [XactCallbackItem](XactCallbackItem.md) (self-reference for linked list)
  - [callback](../c/callback.md) (function pointer field)
- Called from (representative examples):
  - [RegisterXactCallback](../R/RegisterXactCallback.md)
  - [UnregisterXactCallback](../U/UnregisterXactCallback.md)
  - [CallXactCallbacks](../C/CallXactCallbacks.md)

## Notes and Other Information
The callback mechanism supports both start-of-transaction and end-of-transaction events, allowing registered functions to perform initialization and cleanup operations. The generic void pointer argument enables callbacks to maintain context-specific state information. This design is commonly used by PostgreSQL's resource management systems, background worker processes, and third-party extensions that need to coordinate with transaction lifecycle events.