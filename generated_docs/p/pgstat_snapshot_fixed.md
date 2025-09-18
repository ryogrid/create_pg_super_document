# pgstat_snapshot_fixed

## Location
src/backend/utils/activity/pgstat.c: 940 - 956

## Overview
Ensures that a snapshot for a fixed-numbered statistics kind exists, creating it if necessary through either full or partial snapshot building mechanisms.

## Definition


## Detailed Description
This function guarantees that a statistics snapshot exists for the specified fixed-numbered statistics kind. It serves as the entry point for accessing fixed-amount statistics by ensuring the appropriate snapshot data is available before statistics retrieval functions can access it. The function implements two snapshot building strategies based on the current fetch consistency setting: either building a complete snapshot for all statistics kinds or building a targeted snapshot for only the requested kind.

The function performs validation to ensure the requested kind is valid and represents a fixed-amount statistics type. It handles snapshot clearing when forced, and delegates to appropriate snapshot building functions based on the configured consistency level.

## Parameters / Member Variables
- : The statistics kind (PgStat_Kind) for which to ensure a snapshot exists. Must be a valid, fixed-amount statistics kind.

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_is_kind_valid
  - [pgstat_get_kind_info](pgstat_get_kind_info.md)
  - [pgstat_clear_snapshot](pgstat_clear_snapshot.md)
  - [pgstat_build_snapshot](pgstat_build_snapshot.md)
  - [pgstat_build_snapshot_fixed](pgstat_build_snapshot_fixed.md)
  - PGSTAT_FETCH_CONSISTENCY_SNAPSHOT
- Called from (representative examples):
  - [pgstat_fetch_stat_archiver](pgstat_fetch_stat_archiver.md)
  - [pgstat_fetch_stat_bgwriter](pgstat_fetch_stat_bgwriter.md)
  - [pgstat_fetch_stat_checkpointer](pgstat_fetch_stat_checkpointer.md)
  - [pgstat_fetch_stat_io](pgstat_fetch_stat_io.md)
  - [pgstat_fetch_slru](pgstat_fetch_slru.md)
  - pgstat_fetch_stat_wal

## Notes and Other Information
- The function includes assertions to validate that the kind is valid and represents fixed-amount statistics
- Supports forced snapshot clearing through the force_stats_snapshot_clear flag
- Uses different snapshot building strategies based on fetch consistency settings
- Ensures the snapshot validity flag is set for the requested kind after completion
- Primarily used by pgstat_fetch_* functions as a prerequisite for statistics data access