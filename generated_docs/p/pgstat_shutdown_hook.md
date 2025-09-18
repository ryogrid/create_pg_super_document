# pgstat_shutdown_hook

## Location
src/backend/utils/activity/pgstat.c: 503 - 536

## Overview
Handles individual backend shutdown by flushing remaining statistics and cleaning up statistics-related resources. This function ensures that operations triggered during backend exit are properly counted.

## Definition


## Detailed Description
This function is called as a process exit hook to properly shut down a single backend's statistics reporting. It ensures that any final statistics operations that occur during backend termination (such as temporary table deletions) are properly recorded and reported before the process exits.

The function performs several critical cleanup operations:

1. **Database disconnect reporting**: If the backend has discovered its database ID, it reports the disconnection to track session termination statistics. This includes categorizing the disconnect type (normal, client EOF, fatal error, or killed).

2. **Final statistics flush**: Forces a complete flush of all pending statistics updates using  to ensure no statistics are lost.

3. **Pending statistics verification**: Asserts that no statistics remain pending after the flush, then reinitializes the pending list to ensure clean state.

4. **Shared memory cleanup**: Detaches from the statistics shared memory area, releasing hash table references and DSA resources.

5. **Shutdown state marking**: In debug builds, marks the statistics system as shut down to catch any inappropriate subsequent usage.

The function includes safety assertions to verify it's called in the correct context:
- Ensures the statistics system hasn't already been shut down
- Verifies it's running in a backend process under the postmaster or in single-user mode

## Parameters / Member Variables
- : Process exit code (unused but required for exit hook signature)  
- : Datum argument (unused but required for exit hook signature)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_report_disconnect](pgstat_report_disconnect.md)
  - [pgstat_report_stat](pgstat_report_stat.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [dlist_init](../d/dlist_init.md)
  - pgstat_detach_shmem
- Called from (representative examples):
  - [pgstat_initialize](pgstat_initialize.md) (src/backend/utils/activity/pgstat.c:546) - registered as exit hook

## Notes and Other Information
- This is a static function used internally by the statistics system
- Registered as an exit hook during statistics system initialization
- Ensures operations during backend exit (like temp table cleanup) are counted
- Different from  which handles entire server shutdown
- Database disconnect tracking includes categorization by termination cause
- In debug builds, sets a flag to prevent further statistics operations after shutdown
- Critical for maintaining statistics accuracy across backend lifecycle
- The function is located in src/backend/utils/activity/pgstat.c:503-536