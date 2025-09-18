# UnregisterSubXactCallback

## Location
[src/backend/access/transam/xact.c:3826-3846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3826-L3846)

## Overview
UnregisterSubXactCallback removes a previously registered subtransaction callback function from the callback chain, allowing modules to stop receiving subtransaction event notifications.

## Definition


## Detailed Description
This function searches through the linked list of subtransaction callbacks (SubXact_callbacks) to find and remove a specific callback function. It performs a linear search through the callback chain, matching both the callback function pointer and the argument pointer. Once found, the callback item is removed from the linked list and its memory is freed using pfree(). This is the counterpart to RegisterSubXactCallback and is typically called during module cleanup or when a module no longer needs to be notified of subtransaction events.

## Parameters / Member Variables
- : Function pointer to the SubXactCallback function that was previously registered
- : Pointer to the argument data that was associated with the callback during registration

## Dependencies
- Functions called/Symbols referenced:
  - [SubXactCallbackItem](../S/SubXactCallbackItem.md) (structure type)
  - [pfree](../p/pfree.md) (memory deallocation function)
  - SubXact_callbacks (global callback list head)
- Called from (representative examples):
  - No direct references found in the current codebase

## Notes and Other Information
- The function uses both callback function pointer and argument pointer for matching to ensure precise callback removal
- If multiple callbacks with the same function pointer exist, only the one with matching argument is removed
- The function breaks after removing the first match, assuming unique callback/argument pairs
- Memory management is handled automatically through pfree() call
- No error is reported if the callback is not found in the list