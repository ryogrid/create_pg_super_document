# pgstat_drop_all_entries

## Location
[src/backend/utils/activity/pgstat_shmem.c:971-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L971-L992)

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
  - [dshash_seq_init](../d/dshash_seq_init.md)
  - [dshash_seq_next](../d/dshash_seq_next.md)
  - [pgstat_drop_entry_internal](pgstat_drop_entry_internal.md)
  - [dshash_seq_term](../d/dshash_seq_term.md)
  - [pgstat_request_entry_refs_gc](pgstat_request_entry_refs_gc.md)
- Types used:
  - [dshash_seq_status](../d/dshash_seq_status.md)
  - [PgStatShared_HashEntry](../P/PgStatShared_HashEntry.md)
- Called from:
  - [pgstat_reset_after_failure](pgstat_reset_after_failure.md)

## Notes and Other Information
- This is a complete reset function that clears all statistics data
- Uses exclusive locking on the shared hash table during iteration
- Implements garbage collection signaling for entries that cannot be immediately freed
- Part of PostgreSQL's failure recovery and statistics reset infrastructure
- Primarily used during error recovery scenarios
- Location: src/backend/utils/activity/pgstat_shmem.c:971-992