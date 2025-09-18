# pgstat_progress_update_multi_param

## Location
src/backend/utils/activity/backend_progress.c: 122 - 150

## Overview
Atomically updates multiple progress parameters in a single operation, ensuring readers see a consistent snapshot of all related progress metrics simultaneously.

## Definition
```c
void pgstat_progress_update_multi_param(int nparam, const int *index, const int64 *val)
```

## Detailed Description
This function provides atomic updates for multiple progress parameters within a single write activity transaction. This is crucial when multiple progress metrics are logically related and should be updated together to maintain consistency for external observers monitoring the progress.

The function takes arrays of indices and corresponding values, allowing efficient bulk updates while ensuring that readers of the progress information never see an inconsistent intermediate state where some parameters have been updated but others have not. This is particularly important for progress reporting where multiple metrics together provide meaningful information (e.g., current block number and total blocks processed).

All array indices are validated to ensure they fall within the valid range, and the entire operation is protected by atomic write operations to maintain data consistency.

## Parameters / Member Variables
- `nparam`: The number of parameters to update. If 0, the function returns immediately without taking any action
- `index`: Array of parameter indices (0-based) to update. Each must be between 0 and PGSTAT_NUM_PROGRESS_PARAM-1
- `val`: Array of corresponding 64-bit integer values to store at the specified indices

## Dependencies
- Functions called/Symbols referenced:
  - PgBackendStatus (struct type)
  - PGSTAT_BEGIN_WRITE_ACTIVITY (macro)
  - PGSTAT_NUM_PROGRESS_PARAM (constant)
  - PGSTAT_END_WRITE_ACTIVITY (macro)
- Called from (representative examples):
  - lazy_scan_heap (VACUUM progress with multiple related metrics)
  - index_build (CREATE INDEX progress with multiple phases)
  - heapam_relation_copy_for_cluster (CLUSTER progress tracking)
  - validate_index (index validation progress)
  - WaitForLockersMultiple (lock waiting progress)

## Notes and Other Information
- Provides atomicity guarantee - readers will never see partial updates across multiple parameters
- Includes assertions to validate that all indices are within the valid range [0, PGSTAT_NUM_PROGRESS_PARAM)
- Returns immediately if the backend entry is not available, progress tracking is disabled, or nparam is 0
- More efficient than multiple individual calls to pgstat_progress_update_param when updating related metrics
- Commonly used when transitioning between operation phases where multiple progress counters need simultaneous updates
- Essential for maintaining logical consistency in progress reporting for complex operations
- The arrays must have at least `nparam` elements to avoid buffer overruns