# pgstat_reset_of_kind

## Location
src/backend/utils/activity/pgstat.c: 755 - 780

## Overview
This function resets statistics for all entries of a specified statistics kind within PostgreSQL's statistics collection system.

## Definition


## Detailed Description
The  function provides a mechanism to reset statistics for all entries belonging to a specific statistics kind. It operates by first retrieving the kind information for the specified statistics type, then calling the appropriate reset mechanism based on whether the kind has a fixed amount of entries or a variable amount.

For statistics kinds with a fixed amount of entries (like shared statistics), it calls the kind-specific reset callback function directly. For statistics kinds with variable entries, it delegates to  to handle the reset operation across all entries of that kind.

The function captures the current timestamp to mark when the reset operation occurred, ensuring that statistics consumers can understand the temporal context of the reset.

## Parameters / Member Variables
- : A  enum value specifying which type of statistics to reset (e.g., database stats, table stats, function stats, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_kind_info
  - GetCurrentTimestamp
  - pgstat_reset_entries_of_kind
  - PgStat_Kind (enum type)
  - PgStat_KindInfo (struct type)
- Called from (representative examples):
  - pg_stat_reset_shared
  - pg_stat_reset_slru
  - pg_stat_reset_replication_slot
  - pg_stat_reset_subscription_stats

## Notes and Other Information
- Permission checking for this function is managed through the normal PostgreSQL GRANT system, as noted in the source comments
- The function handles both fixed-amount statistics kinds (using direct callbacks) and variable-amount kinds (using entry-based reset mechanisms)
- This is a core function in PostgreSQL's statistics reset infrastructure, providing a unified interface for resetting different types of statistics
- The timestamp captured during reset helps maintain consistency in the statistics system timeline