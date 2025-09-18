# UnregisterXactCallback

## Location
src/backend/access/transam/xact.c: 3766 - 3786

## Overview
Removes a previously registered transaction callback function from the callback list.

## Definition
void UnregisterXactCallback(XactCallback callback, void *arg)

## Detailed Description
This function removes a transaction callback that was previously registered with RegisterXactCallback. It searches through the linked list of XactCallbackItem structures to find an entry that matches both the callback function pointer and the argument pointer exactly.

The function performs a linear search through the Xact_callbacks linked list, comparing both the callback function and the argument for exact matches. When a match is found, the item is removed from the list by updating the linked list pointers and the memory is freed using pfree().

This is the counterpart to RegisterXactCallback and is typically used by dynamically loaded modules when they are being unloaded or when they no longer need to receive transaction notifications.

## Parameters / Member Variables
- callback: Function pointer of type XactCallback that was previously registered
- arg: User-defined argument pointer that was passed during registration (must match exactly)

## Dependencies
- Functions called/Symbols referenced:
  - XactCallbackItem (structure type)
  - Xact_callbacks (global linked list head)
  - pfree (for deallocating callback item memory)
- Called from (representative examples):
  - No direct references found in the codebase (used by dynamically loaded modules)

## Notes and Other Information
- Both callback function pointer and argument must match exactly for removal
- Performs linear search through the callback list until first match is found
- Only removes the first matching entry if multiple identical registrations exist
- Memory is properly freed using pfree() to prevent memory leaks
- Designed for use by dynamically loaded modules during cleanup or unloading
- Safe to call even if the callback was not previously registered (no-op in that case)
- Maintains the integrity of the linked list structure during removal