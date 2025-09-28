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
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md): Gets the current timestamp for reset operations
  - [pgstat_get_kind_info](pgstat_get_kind_info.md): Retrieves metadata for each statistics kind
  - [pgstat_drop_all_entries](pgstat_drop_all_entries.md): Removes all variable-numbered statistics entries
  - reset_all_cb: Callback function pointer for resetting fixed statistics (via kind_info)

- Called from (representative examples):
  - [pgstat_read_statsfile](pgstat_read_statsfile.md): Called when statistics file reading fails or file is corrupted
  - [pgstat_discard_stats](pgstat_discard_stats.md): Called when discarding statistics is required

## Notes and Other Information
- This function is designed to be safe to call even when the statistics system is in a partially initialized state
- The timestamp parameter passed to reset callbacks ensures consistent timing for all reset operations
- Only processes statistics kinds that have a fixed amount (fixed_amount flag set)
- [Variable](../V/Variable.md)-numbered statistics (like per-table or per-function stats) are completely dropped rather than reset
- Critical for maintaining system stability when statistics corruption is detected
- Part of the defensive programming approach in PostgreSQL's statistics subsystem
- Located in src/backend/utils/activity/pgstat.c:1694-1716

## Simplified Source

```c
// Simplified version of pgstat_reset_after_failure
static void pgstat_reset_after_failure(void) {
    TimestampTz ts = GetCurrentTimestamp();

    // Step 1: Reset all fixed-numbered statistics
    for (int kind = PGSTAT_KIND_FIRST_VALID; kind <= PGSTAT_KIND_LAST; kind++) {
        const PgStat_KindInfo *kind_info = pgstat_get_kind_info(kind);

        // Only process statistics kinds with fixed amounts
        if (!kind_info->fixed_amount)
            continue;

        // Call the reset callback for this statistics kind
        kind_info->reset_all_cb(ts);
    }

    // Step 2: Drop all variable-numbered statistics entries
    pgstat_drop_all_entries();
}
```

Key simplifications made:
- Added clear step-by-step comments for the two-phase reset process
- Explained the fixed vs variable statistics distinction
- Maintained the essential reset and cleanup logic
- Preserved the callback-based architecture