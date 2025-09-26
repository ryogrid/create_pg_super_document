# pgstat_reset_after_failure

## Location
[src/backend/utils/activity/pgstat.c:1694-1716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1694-L1716)

## Overview
The  function provides a recovery mechanism that resets all statistics to a clean state after a crash or when statistics file restoration fails.

## Definition

```c
static void
pgstat_reset_after_failure(void)
```
## Detailed Description
This function serves as a critical recovery mechanism in PostgreSQL's statistics system. It ensures that the statistics subsystem can recover gracefully from failures such as crashes, corrupted statistics files, or partial loading failures during system startup.

The function performs a two-phase reset operation:
1. **Fixed-numbered statistics reset**: Iterates through all valid statistics kinds that have a fixed amount and calls their respective reset callbacks with the current timestamp
2. **Variable-numbered statistics cleanup**: Drops all variable-numbered statistics entries from the shared hash table

This comprehensive approach ensures that no stale or potentially corrupted statistics remain in memory after a failure, allowing PostgreSQL to continue operating with a clean statistics slate.

## Parameters / Member Variables
This function takes no parameters and operates on global statistics state.

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp: Gets the current timestamp for reset operations
  - pgstat_get_kind_info: Retrieves metadata for each statistics kind
  - pgstat_drop_all_entries: Removes all variable-numbered statistics entries
  - reset_all_cb: Callback function pointer for resetting fixed statistics (via kind_info)

- Called from (representative examples):
  - pgstat_read_statsfile: Called when statistics file reading fails or file is corrupted
  - pgstat_discard_stats: Called when discarding statistics is required

## Notes and Other Information
- This function is designed to be safe to call even when the statistics system is in a partially initialized state
- The timestamp parameter passed to reset callbacks ensures consistent timing for all reset operations
- Only processes statistics kinds that have a fixed amount (fixed_amount flag set)
- Variable-numbered statistics (like per-table or per-function stats) are completely dropped rather than reset
- Critical for maintaining system stability when statistics corruption is detected
- Part of the defensive programming approach in PostgreSQL's statistics subsystem
- Located in src/backend/utils/activity/pgstat.c:1694-1716