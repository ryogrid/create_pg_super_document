# MemoryContextRegisterResetCallback

## Location
[src/backend/utils/mmgr/mcxt.c:568-584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L568-L584)

## Overview
Registers a function to be called before the next context reset/delete, with callbacks executed in reverse order of registration.

## Definition


## Detailed Description
MemoryContextRegisterResetCallback provides a mechanism for registering cleanup functions that will be automatically executed before a memory context is reset or deleted. This callback system allows code to perform necessary cleanup operations, such as closing files, releasing resources, or updating global state, before the context's memory is freed.

The function maintains callbacks in a linked list attached to the context, with new callbacks inserted at the head. This results in a LIFO (Last In, First Out) execution order, meaning the most recently registered callbacks are executed first during reset/delete operations.

The caller must provide a pre-allocated MemoryContextCallback structure with the function pointer and argument properly initialized. The documentation recommends allocating this callback structure within the same context being monitored, ensuring automatic cleanup when the context is destroyed.

Importantly, there is no deregistration API - once a callback is registered, it cannot be removed. If conditional behavior is needed, the callback function should check state in its argument to determine whether to perform any actions.

## Parameters / Member Variables
- : The memory context to attach the callback to. Must be a valid MemoryContext.
- : Pointer to a MemoryContextCallback structure containing the function to call and its argument.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextCallback](MemoryContextCallback.md) (structure type)
  - MemoryContextIsValid
- Called from (representative examples):
  - [pgoutput_startup](../p/pgoutput_startup.md)
  - [make_expanded_record_from_typeid](../m/make_expanded_record_from_typeid.md)
  - [InitDomainConstraintRef](../I/InitDomainConstraintRef.md)
  - [PLy_exec_function](../P/PLy_exec_function.md)

## Notes and Other Information
- Callbacks are executed in reverse order of registration (LIFO - Last In, First Out)
- The caller is responsible for allocating and initializing the MemoryContextCallback structure
- No deregistration API exists - callbacks remain active until context destruction
- The context is marked as non-reset when a callback is registered
- Callback structures should typically be allocated within the monitored context for automatic cleanup
- Used extensively for resource management in replication, type caching, and procedural language implementations
- The callback system provides a clean way to ensure cleanup operations occur before memory is freed