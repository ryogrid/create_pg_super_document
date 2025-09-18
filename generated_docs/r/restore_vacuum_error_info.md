# restore_vacuum_error_info

## Location
src/backend/access/heap/vacuumlazy.c: 3189 - 3195

## Overview
Restores vacuum error information from a previously saved state, complementing the save functionality of update_vacuum_error_info.

## Definition


## Detailed Description
The `restore_vacuum_error_info` function restores the vacuum error context from a previously saved state. It is the complement to `update_vacuum_error_info` and is used to revert the error context to a previous state after temporary changes have been made during nested vacuum operations.

This function ensures that error context information remains consistent and accurate when vacuum operations involve multiple phases or nested operations that temporarily change the error context. By restoring the saved values for block number, offset number, and phase, it maintains the proper error reporting hierarchy.

## Parameters / Member Variables
- `vacrel`: Pointer to the current `LVRelState` structure where error information will be restored
- `saved_vacrel`: Pointer to the `LVSavedErrInfo` structure containing the previously saved error state (marked as const since it's only read from)

## Dependencies
- Functions called/Symbols referenced:
  - `[LVRelState](../L/LVRelState.md)` (vacuum relation state structure)
  - `[LVSavedErrInfo](../L/LVSavedErrInfo.md)` (saved error info structure)
- Called from (representative examples):
  - `[lazy_vacuum_heap_rel](../l/lazy_vacuum_heap_rel.md)` (src/backend/access/heap/vacuumlazy.c:2183)
  - `[lazy_vacuum_heap_page](../l/lazy_vacuum_heap_page.md)` (src/backend/access/heap/vacuumlazy.c:2284)
  - `[lazy_vacuum_one_index](../l/lazy_vacuum_one_index.md)` (src/backend/access/heap/vacuumlazy.c:2453)
  - `[lazy_cleanup_one_index](../l/lazy_cleanup_one_index.md)` (src/backend/access/heap/vacuumlazy.c:2502)

## Notes and Other Information
- Must be used with error state previously saved by `update_vacuum_error_info`
- The saved_vacrel parameter is const, indicating it's only read from during restoration
- Restores all three components of error state: block number, offset number, and phase
- Essential for maintaining proper error context in nested vacuum operations
- The function is static and only used within the vacuumlazy.c module
- Simple implementation that directly copies saved values back to the active error context