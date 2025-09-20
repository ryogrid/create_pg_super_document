# SubXactCallbackItem

## Location
[src/backend/access/transam/xact.c:317-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L317-L322)

## Overview
SubXactCallbackItem is a linked list node structure that manages subtransaction callback functions, enabling modules to register for start-of-subtransaction and end-of-subtransaction notifications.

## Definition

```c
typedef struct SubXactCallbackItem
{
	struct SubXactCallbackItem *next;
	SubXactCallback callback;
	void	   *arg;
} SubXactCallbackItem;
```
## Detailed Description
SubXactCallbackItem implements a linked list mechanism specifically designed for subtransaction lifecycle management. Similar to XactCallbackItem but focused on subtransaction events, this structure allows PostgreSQL subsystems and extensions to register callback functions that are invoked during subtransaction start, commit, and abort operations. This fine-grained callback system is essential for managing resources and state that must be coordinated with savepoint operations and nested transaction boundaries.

## Parameters / Member Variables
- `*next`: Pointer to the next callback item in the linked list
- `callback`: Function pointer to the subtransaction callback function (SubXactCallback type)
- `*arg`: Generic void pointer to user-defined argument data passed to the callback
## Dependencies
- Functions called/Symbols referenced:
  - [SubXactCallbackItem](SubXactCallbackItem.md) (self-reference for linked list)
  - [callback](../c/callback.md) (function pointer field)
- Called from (representative examples):
  - [RegisterSubXactCallback](../R/RegisterSubXactCallback.md)
  - [UnregisterSubXactCallback](../U/UnregisterSubXactCallback.md)
  - [CallSubXactCallbacks](../C/CallSubXactCallbacks.md)

## Notes and Other Information
The subtransaction callback mechanism is crucial for PostgreSQL's savepoint functionality and nested transaction support. Unlike main transaction callbacks, subtransaction callbacks must handle more complex scenarios including partial rollbacks, cascading aborts, and savepoint releases. This structure enables proper resource cleanup and state management during complex transaction nesting scenarios, making it essential for features like cursor management, temporary object cleanup, and lock management within subtransactions.