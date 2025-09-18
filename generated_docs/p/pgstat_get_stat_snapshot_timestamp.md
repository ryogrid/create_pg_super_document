# pgstat_get_stat_snapshot_timestamp

## Location
src/backend/utils/activity/pgstat.c: 907 - 923

## Overview
This function returns the timestamp of a statistics snapshot if one exists, providing information about when the current snapshot was taken.

## Definition


## Detailed Description
The  function provides a way to determine whether a statistics snapshot currently exists and, if so, when it was created. This function is particularly useful for understanding the temporal context of statistics data and determining the freshness of the current snapshot.

The function first checks if a forced snapshot clear is pending and performs the clear if necessary. It then examines the current snapshot mode to determine if a full snapshot is active. If operating in snapshot consistency mode, it returns the snapshot timestamp and sets the have_snapshot flag to true. Otherwise, it indicates that no snapshot exists.

This function serves as a diagnostic and informational tool within the statistics system, allowing other components to understand the state and timing of the statistics snapshot.

## Parameters / Member Variables
- : A pointer to a boolean that will be set to true if a snapshot exists, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_clear_snapshot
  - PGSTAT_FETCH_CONSISTENCY_SNAPSHOT (constant)
- Called from (representative examples):
  - No direct callers found in the current codebase analysis

## Notes and Other Information
- This function automatically handles forced snapshot clears when the  flag is set
- The function only returns a valid timestamp when operating in PGSTAT_FETCH_CONSISTENCY_SNAPSHOT mode
- Returns 0 as the timestamp when no snapshot exists, which is a safe sentinel value
- The function is primarily used for diagnostic and monitoring purposes within the statistics infrastructure
- The snapshot timestamp represents when the snapshot was created, not when individual statistics were last updated