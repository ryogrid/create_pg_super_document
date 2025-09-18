# reset_on_dsm_detach

## Location
src/backend/storage/ipc/dsm.c: 1170 - 1200

## Overview
Discards all registered on-detach callbacks without executing them, effectively cleaning up callback registration for all active DSM segments.

## Definition
```c
void reset_on_dsm_detach(void)
```

## Detailed Description
This function performs cleanup of on-detach callback registrations for all active dynamic shared memory (DSM) segments without actually executing the callbacks. It iterates through the global DSM segment list and for each segment:

1. Removes all registered on-detach callbacks from the segment's callback list
2. Frees the memory associated with each callback structure
3. Invalidates the control slot to prevent implicit reference count decrementation

This function is typically called during process cleanup scenarios where callbacks should not be executed but their registrations need to be cleared to avoid memory leaks.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach
  - dlist_container
  - slist_is_empty
  - slist_pop_head_node
  - slist_container
  - pfree
  - INVALID_CONTROL_SLOT
- Called from (representative examples):
  - on_exit_reset

## Notes and Other Information
- This is a cleanup function that deliberately avoids executing registered callbacks
- Sets control_slot to INVALID_CONTROL_SLOT to prevent reference count decrementation
- Used during process termination or error recovery scenarios
- Located in src/backend/storage/ipc/dsm.c:1170-1200