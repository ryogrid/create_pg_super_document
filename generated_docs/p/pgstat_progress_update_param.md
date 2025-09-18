# pgstat_progress_update_param

## Location
src/backend/utils/activity/backend_progress.c: 49 - 69

## Overview
Updates a specific progress parameter in the backend's progress tracking array to report incremental progress during long-running PostgreSQL operations.

## Definition
```c
void pgstat_progress_update_param(int index, int64 val)
```

## Detailed Description
This function is the primary mechanism for updating individual progress parameters during the execution of trackable PostgreSQL commands. It allows operations to report specific metrics such as number of blocks processed, tuples scanned, or completion percentages by updating a designated slot in the `st_progress_param` array.

The function performs bounds checking to ensure the index is within the valid range and uses atomic write operations to maintain consistency when updating the backend status information that can be monitored by other processes or tools like `pg_stat_progress_*` views.

Each command type typically defines its own set of progress parameters (e.g., VACUUM tracks heap blocks scanned, index vacuum cycles, etc.) and uses specific array indices to report different metrics.

## Parameters / Member Variables
- `index`: The array index (0-based) in the progress parameter array to update. Must be between 0 and PGSTAT_NUM_PROGRESS_PARAM-1
- `val`: The new 64-bit integer value to store at the specified index position

## Dependencies
- Functions called/Symbols referenced:
  - [PgBackendStatus](../P/PgBackendStatus.md) (struct type)
  - PGSTAT_NUM_PROGRESS_PARAM (constant)
  - PGSTAT_BEGIN_WRITE_ACTIVITY (macro)
  - PGSTAT_END_WRITE_ACTIVITY (macro)
- Called from (representative examples):
  - [lazy_scan_heap](../l/lazy_scan_heap.md) (VACUUM progress reporting)
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (ANALYZE progress reporting)
  - [DefineIndex](../D/DefineIndex.md) (CREATE INDEX progress reporting)
  - [CopyFrom](../C/CopyFrom.md)/DoCopyTo (COPY progress reporting)
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md) (CLUSTER progress reporting)

## Notes and Other Information
- Includes an assertion to validate that the index is within the valid range [0, PGSTAT_NUM_PROGRESS_PARAM)
- Returns immediately if the backend entry is not available or progress tracking is disabled
- Uses atomic write operations to ensure data consistency when multiple processes may be reading the progress information
- This is the most frequently called progress tracking function during operation execution
- Different command types use different indices for different metrics (e.g., blocks processed vs. tuples processed)
- The function is called repeatedly throughout long-running operations to provide real-time progress updates