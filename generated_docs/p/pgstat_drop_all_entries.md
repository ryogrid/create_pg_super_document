# pgstat_drop_all_entries

## Location
src/backend/utils/activity/pgstat_shmem.c: 971 - 992

## Overview
This function drops all statistics entries from the shared statistics hash table, effectively clearing all collected statistics data.

## Definition
```c
void pgstat_drop_all_entries(void)
```

## Detailed Description
The `pgstat_drop_all_entries` function performs a complete cleanup of the shared statistics hash table by iterating through all entries and attempting to drop each one that hasn't already been marked as dropped. It uses exclusive locking during the iteration to ensure thread safety. The function counts entries that cannot be immediately freed and requests garbage collection for cached references when needed, similar to other drop functions in the statistics subsystem.

## Parameters / Member Variables
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - dshash_seq_init
  - dshash_seq_next
  - pgstat_drop_entry_internal
  - dshash_seq_term
  - pgstat_request_entry_refs_gc
- Types used:
  - dshash_seq_status
  - PgStatShared_HashEntry
- Called from:
  - pgstat_reset_after_failure

## Notes and Other Information
- This is a complete reset function that clears all statistics data
- Uses exclusive locking on the shared hash table during iteration
- Implements garbage collection signaling for entries that cannot be immediately freed
- Part of PostgreSQL's failure recovery and statistics reset infrastructure
- Primarily used during error recovery scenarios
- Location: src/backend/utils/activity/pgstat_shmem.c:971-992