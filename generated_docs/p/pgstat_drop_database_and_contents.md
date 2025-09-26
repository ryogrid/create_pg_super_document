# pgstat_drop_database_and_contents

## Location
src/backend/utils/activity/pgstat_shmem.c: 866 - 926

## Overview
This function drops statistics for a database and all objects contained within that database from the shared statistics hash table.

## Definition

```c
struct PgStat_HashKey));
```
## Detailed Description
The  function performs a comprehensive cleanup of statistics data for a specific database. It iterates through the shared statistics hash table and removes all entries that belong to the specified database OID. The function implements a two-phase approach: first releasing local backend references to prevent cleanup delays, then performing the actual removal while holding appropriate locks.

The function handles cases where statistics entries cannot be immediately freed (for example, when they are still being accessed by other backends) by incrementing a counter and requesting garbage collection of cached references when needed.

## Parameters / Member Variables
- : The Object Identifier (OID) of the database whose statistics entries should be dropped

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_release_db_entry_refs
  - dshash_seq_init
  - dshash_seq_next
  - pgstat_drop_entry_internal
  - dshash_seq_term
  - pgstat_request_entry_refs_gc
- Types used:
  - dshash_seq_status
  - PgStatShared_HashEntry
- Called from:
  - pgstat_drop_entry

## Notes and Other Information
- This is a static function internal to pgstat_shmem.c
- Uses exclusive locking on the shared hash table during iteration to ensure thread safety
- Implements garbage collection signaling for entries that cannot be immediately freed
- Part of PostgreSQL's statistics collection infrastructure
- Location: src/backend/utils/activity/pgstat_shmem.c:866-926