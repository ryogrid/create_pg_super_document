# pgstat_reset_entry

## Location
[src/backend/utils/activity/pgstat_shmem.c:1009-1028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L1009-L1028)

## Overview
This function resets a single variable-numbered statistics entry, clearing its collected data while preserving the entry structure.

## Definition
```c
void pgstat_reset_entry(PgStat_Kind kind, Oid dboid, Oid objoid, TimestampTz ts)
```

## Detailed Description
The `pgstat_reset_entry` function resets the contents of a specific statistics entry identified by its kind, database OID, and object OID. It first validates that the statistics kind supports variable-numbered entries (not fixed-amount entries), then retrieves a reference to the entry. If the entry exists and hasn't been dropped, it acquires a lock on the entry, calls the shared reset function to zero the data and update timestamps, and then releases the lock. This provides a thread-safe way to reset individual statistics entries.

## Parameters / Member Variables
- `kind`: The type of statistics entry to reset (PgStat_Kind enum value)
- `dboid`: The Object Identifier of the database containing the entry
- `objoid`: The Object Identifier of the specific object whose statistics are being reset
- `ts`: The timestamp to record as the reset time

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_kind_info
  - pgstat_get_entry_ref
  - pgstat_lock_entry
  - shared_stat_reset_contents
  - pgstat_unlock_entry
- Types used:
  - PgStat_Kind
  - PgStat_EntryRef
  - TimestampTz
- Called from:
  - pgstat_reset
  - pgstat_create_subscription

## Notes and Other Information
- Only works with variable-numbered statistics entries, not fixed-amount entries
- Implements proper locking to ensure thread safety during reset operations
- Returns early if the entry doesn't exist or has been marked as dropped
- Part of PostgreSQL's statistics reset and management infrastructure
- Used for selective statistics clearing rather than bulk operations
- Location: src/backend/utils/activity/pgstat_shmem.c:1009-1028