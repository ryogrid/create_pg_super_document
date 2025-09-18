# update_vacuum_error_info

## Location
src/backend/access/heap/vacuumlazy.c: 3170 - 3188

## Overview
Updates the vacuum error information for error callbacks, while optionally saving the current error state for later restoration.

## Definition


## Detailed Description
The `update_vacuum_error_info` function manages the error context information used by the vacuum error callback system. It serves a dual purpose: first, it saves the current error state to a provided save structure (if one is provided), and second, it updates the current error context with new phase, block number, and offset number information.

This function is essential for maintaining accurate error context during vacuum operations, allowing the system to provide precise error messages that identify exactly where in the vacuum process an error occurred. The ability to save and restore error state is crucial for nested operations where temporary context changes need to be reverted.

## Parameters / Member Variables
- `vacrel`: Pointer to the current `LVRelState` structure containing vacuum error information
- `saved_vacrel`: Optional pointer to `LVSavedErrInfo` structure where current error state will be saved (can be NULL)
- `phase`: New vacuum phase identifier to set in the error context
- `blkno`: New block number to set in the error context
- `offnum`: New offset number to set in the error context

## Dependencies
- Functions called/Symbols referenced:
  - `LVRelState` (vacuum relation state structure)
  - `LVSavedErrInfo` (saved error info structure)
- Called from (representative examples):
  - `lazy_scan_heap` (src/backend/access/heap/vacuumlazy.c:855)
  - `lazy_vacuum_heap_rel` (src/backend/access/heap/vacuumlazy.c:2124)
  - `lazy_vacuum_heap_page` (src/backend/access/heap/vacuumlazy.c:2211)
  - `lazy_vacuum_one_index` (src/backend/access/heap/vacuumlazy.c:2444)
  - `lazy_cleanup_one_index` (src/backend/access/heap/vacuumlazy.c:2495)
  - `lazy_truncate_heap` (src/backend/access/heap/vacuumlazy.c:2562)

## Notes and Other Information
- The function conditionally saves current error state only if a save structure is provided (non-NULL)
- Always updates the current error context with new values regardless of save operation
- Used extensively throughout vacuum operations to maintain accurate error context
- Works in conjunction with `restore_vacuum_error_info` to provide save/restore functionality
- The function is static and only used within the vacuumlazy.c module