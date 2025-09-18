# pgstat_build_snapshot_fixed

## Location
[src/backend/utils/activity/pgstat.c:1066-1106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1066-L1106)

## Overview
Builds a snapshot for a specific fixed-numbered statistics kind by invoking its registered snapshot callback function with appropriate validity tracking and caching logic.

## Definition


## Detailed Description
This function manages the snapshot building process for fixed-numbered statistics kinds that have a predetermined, fixed amount of statistics entries. It implements intelligent caching and validity tracking to avoid redundant snapshot building while ensuring data freshness based on the configured fetch consistency mode.

The function validates that the requested kind supports fixed-amount statistics and has a registered snapshot callback. It then determines whether to rebuild the snapshot based on the current fetch consistency setting: in NONE mode it always rebuilds, in CACHE mode it respects existing valid snapshots, and in SNAPSHOT mode it assumes the snapshot was already built by the comprehensive snapshot building process.

The actual snapshot building is delegated to the kind-specific callback function, which knows how to construct the appropriate snapshot data structure for that particular statistics type.

## Parameters / Member Variables
- : The fixed-numbered statistics kind (PgStat_Kind) for which to build a snapshot. Must be a valid fixed-amount statistics kind with a registered snapshot callback.

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_kind_info](pgstat_get_kind_info.md)
  - kind_info->snapshot_cb (callback function)
  - PGSTAT_FETCH_CONSISTENCY_NONE
  - PGSTAT_FETCH_CONSISTENCY_CACHE
- Called from (representative examples):
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md)
  - [pgstat_build_snapshot](pgstat_build_snapshot.md)
  - [pgstat_write_statsfile](pgstat_write_statsfile.md)

## Notes and Other Information
- Static function for internal use within the pgstat module
- Includes assertions to validate the kind supports fixed-amount statistics and has a snapshot callback
- Implements different behaviors based on fetch consistency modes to optimize performance
- Uses validity flags to track snapshot state and prevent redundant rebuilding
- The snapshot callback is responsible for the actual data collection and snapshot creation
- Maintains consistency by clearing and setting validity flags appropriately
- Used both for individual snapshot building and as part of comprehensive snapshot operations