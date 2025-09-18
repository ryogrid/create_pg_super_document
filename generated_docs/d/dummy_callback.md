# dummy_callback

## Location
src/backend/access/spgist/spgvacuum.c: 936 - 946

## Overview
A no-op callback function that always returns false, used during SP-GiST vacuum cleanup to perform scanning without deleting any tuples.

## Definition


## Detailed Description
The `dummy_callback` function is a minimal implementation of the `IndexBulkDeleteCallback` interface that serves as a placeholder during SP-GiST vacuum cleanup operations. It always returns false, indicating that no tuple should be deleted regardless of the input.

This callback is specifically designed for use with `spgvacuumcleanup` when the goal is to perform index maintenance operations (such as updating statistics, cleaning up empty pages, and updating the Free Space Map) without actually deleting any tuples. This scenario typically occurs during vacuum operations where no heap tuples have been marked for deletion, but index maintenance is still needed.

## Parameters / Member Variables
- `itemptr`: Pointer to the item (tuple) being considered for deletion (unused in this implementation)
- `state`: Opaque state data passed from the vacuum operation (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple return statement)
- Called from (representative examples):
  - [spgvacuumcleanup](../s/spgvacuumcleanup.md)

## Notes and Other Information
- Part of the IndexBulkDeleteCallback interface contract
- Used specifically for cleanup-only vacuum operations where no tuples should be deleted
- Enables reuse of the bulk delete infrastructure for maintenance-only operations
- The function parameters are ignored since the function always returns the same result
- Essential for separating tuple deletion logic from index maintenance logic in SP-GiST vacuum