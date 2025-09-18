# RegisterResourceReleaseCallback

## Location
[src/backend/utils/resowner/resowner.c:944-957](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L944-L957)

## Overview
Registers a callback function that will be invoked during resource cleanup phases, allowing dynamically loaded modules to participate in PostgreSQL's resource management system.

## Definition


Where ResourceReleaseCallback is defined as:


## Detailed Description
This function allows dynamically loaded modules (extensions) to register custom cleanup callbacks that will be executed during resource owner cleanup. The function allocates memory in TopMemoryContext to store the callback information and adds it to a global linked list of resource release callbacks. When resource cleanup occurs, all registered callbacks will be invoked in the reverse order of registration.

This is part of PostgreSQL's legacy callback system for resource management. While still supported for backward compatibility, newer extensions are encouraged to define custom ResourceOwnerDesc structures with specific callbacks instead.

## Parameters / Member Variables
- : Function pointer to the cleanup callback that will be invoked during resource release phases
- : User-defined argument that will be passed to the callback function when invoked

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - ResourceReleaseCallbackItem (struct type)
  - TopMemoryContext (global variable)
  - ResourceRelease_callbacks (global variable)

- Called from (representative examples):
  - No direct references found in current codebase (used by dynamically loaded modules)

## Notes and Other Information
- Memory for callback items is allocated in TopMemoryContext to ensure it persists for the lifetime of the process
- Callbacks are stored in a simple linked list and executed in reverse order of registration
- This is a legacy interface; new code should prefer defining ResourceOwnerDesc with custom callbacks
- The callback will receive phase information (RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS, RESOURCE_RELEASE_AFTER_LOCKS), commit status, and top-level transaction status
- Extensions using this function should also implement UnregisterResourceReleaseCallback for proper cleanup