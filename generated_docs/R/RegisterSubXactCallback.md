# RegisterSubXactCallback

## Location
src/backend/access/transam/xact.c: 3813 - 3825

## Overview
Registers a callback function to be invoked at subtransaction start and end events for dynamically loaded modules.

## Definition
void RegisterSubXactCallback(SubXactCallback callback, void *arg)

## Detailed Description
This function allows dynamically loaded modules to register callback functions that will be called during subtransaction lifecycle events. Similar to RegisterXactCallback but specifically for subtransaction operations, this callback system enables extensions to hook into savepoint and nested transaction events.

The subtransaction callbacks are invoked at different phases: at subtransaction start (after initialization is complete), post-subcommit, or post-subabort. Like transaction callbacks, subtransaction callbacks can only perform non-critical cleanup operations when called at end events.

The callback registration creates a linked list of SubXactCallbackItem structures stored in TopMemoryContext to ensure they persist across subtransaction boundaries. This allows extensions to track and respond to nested transaction operations.

## Parameters / Member Variables
- callback: Function pointer of type SubXactCallback that will be invoked at subtransaction events
- arg: User-defined argument pointer that will be passed to the callback function when invoked

## Dependencies
- Functions called/Symbols referenced:
  - [SubXactCallbackItem](../S/SubXactCallbackItem.md) (structure type)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (for allocating callback item)
  - TopMemoryContext (memory context for persistent storage)
  - SubXact_callbacks (global linked list head)
- Called from (representative examples):
  - No direct references found in the codebase (used by dynamically loaded modules)

## Notes and Other Information
- Designed specifically for subtransaction events (savepoints, nested transactions)
- Intended for use by dynamically loaded modules and extensions
- Callbacks execute post-subcommit, post-subabort, or at subtransaction start after initialization
- Memory allocation uses TopMemoryContext to ensure callback registrations survive subtransaction boundaries
- Creates a linked list structure with new registrations added at the head
- SubXactCallback type includes subtransaction ID parameters for tracking nested transaction relationships
- Callback signature includes both current and parent subtransaction IDs for context
- Supports events: SUBXACT_EVENT_START_SUB, SUBXACT_EVENT_COMMIT_SUB, SUBXACT_EVENT_ABORT_SUB, SUBXACT_EVENT_PRE_COMMIT_SUB