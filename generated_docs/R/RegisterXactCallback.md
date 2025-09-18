# RegisterXactCallback

## Location
src/backend/access/transam/xact.c: 3753 - 3765

## Overview
Registers a callback function to be invoked at transaction start and end events for dynamically loaded modules.

## Definition
void RegisterXactCallback(XactCallback callback, void *arg)

## Detailed Description
This function allows dynamically loaded modules to register callback functions that will be called during transaction start and end operations. The callback system is primarily designed for extensions and loadable modules, as built-in PostgreSQL modules typically hardwire their transaction handling directly for better control over execution order.

The callback functions are invoked post-commit or post-abort, meaning they can only perform non-critical cleanup operations. The callback registration creates a linked list of XactCallbackItem structures stored in TopMemoryContext to ensure they persist across transaction boundaries.

The callback mechanism provides a way for extensions to hook into transaction lifecycle events without modifying core PostgreSQL code.

## Parameters / Member Variables
- callback: Function pointer of type XactCallback that will be invoked at transaction events
- arg: User-defined argument pointer that will be passed to the callback function when invoked

## Dependencies
- Functions called/Symbols referenced:
  - [XactCallbackItem](../X/XactCallbackItem.md) (structure type)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (for allocating callback item)
  - TopMemoryContext (memory context for persistent storage)
  - Xact_callbacks (global linked list head)
- Called from (representative examples):
  - No direct references found in the codebase (used by dynamically loaded modules)

## Notes and Other Information
- Intended specifically for use by dynamically loaded modules and extensions
- Built-in modules typically use hardwired calls for better order control
- Callbacks execute post-commit or post-abort, restricting them to non-critical cleanup only
- Memory allocation uses TopMemoryContext to ensure callback registrations survive transaction boundaries
- Creates a linked list structure with new registrations added at the head
- The XactCallback type defines the function signature for callback functions