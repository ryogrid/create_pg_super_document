# UnregisterResourceReleaseCallback

## Location
src/backend/utils/resowner/resowner.c: 958 - 981

## Overview
Removes a previously registered resource release callback from the global callback list, allowing dynamically loaded modules to clean up their callback registrations.

## Definition
```c
void UnregisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
```

## Detailed Description
This function searches through the global linked list of registered resource release callbacks and removes the first callback that matches both the callback function pointer and the argument pointer. It performs proper linked list manipulation to maintain the integrity of the callback chain and frees the memory allocated for the callback item.

The function is essential for proper cleanup when dynamically loaded modules are unloaded or when they need to change their resource management behavior. It ensures that dangling callback pointers are not left in the system after a module is unloaded.

## Parameters / Member Variables
- `callback`: Function pointer to the callback that should be unregistered (must match exactly)
- `arg`: User-defined argument pointer that was passed during registration (must match exactly)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation function)
  - ResourceReleaseCallbackItem (struct type)
  - ResourceRelease_callbacks (global linked list head)

- Called from (representative examples):
  - No direct references found in current codebase (used by dynamically loaded modules)

## Notes and Other Information
- Both callback function pointer and argument must match exactly for successful removal
- Only removes the first matching callback if multiple identical registrations exist
- The function performs a linear search through the callback list
- Memory is properly freed using pfree() when a callback is found and removed
- If no matching callback is found, the function returns without error
- Essential for preventing memory leaks and dangling pointers when modules are unloaded
- Should be called in module cleanup routines or when callback behavior needs to be changed