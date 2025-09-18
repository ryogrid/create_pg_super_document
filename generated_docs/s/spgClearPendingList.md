# spgClearPendingList

## Location
src/backend/access/spgist/spgvacuum.c: 89 - 124

## Overview
Clears and deallocates all items from the pending list used during SP-GiST vacuum operations.

## Definition
```c
static void spgClearPendingList(spgBulkDeleteState *bds)
```

## Detailed Description
This function performs cleanup of the pending list by iterating through all pending items and freeing their allocated memory. It includes an assertion to ensure that all items in the list have been properly processed (marked as 'done') before deallocation. After freeing all individual items, it resets the pendingList pointer to NULL, effectively clearing the entire list.

The function is designed to be called at the end of vacuum processing to clean up resources and ensure no memory leaks occur during SP-GiST vacuum operations.

## Parameters / Member Variables
- `bds`: Pointer to spgBulkDeleteState structure containing the vacuum state information, including the pendingList to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - Assert: Debugging assertion macro to verify all items are marked as done
  - pfree: PostgreSQL memory deallocation function
  - spgBulkDeleteState: Structure containing vacuum state
  - spgVacPendingItem: Structure representing a pending vacuum item
- Called from (representative examples):
  - spgprocesspending: Calls this function to clean up the pending list after processing

## Notes and Other Information
- This is a static function, accessible only within the spgvacuum.c file
- The function includes debug assertions to ensure proper processing before cleanup
- All pending items must be marked as 'done' before this function is called, otherwise the assertion will fail in debug builds
- The function prevents memory leaks by properly deallocating all pending items
- After completion, the pendingList is reset to NULL, indicating an empty list
- Part of the SP-GiST vacuum cleanup process