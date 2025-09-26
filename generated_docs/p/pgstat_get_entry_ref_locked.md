# pgstat_get_entry_ref_locked

## Location
src/backend/utils/activity/pgstat_shmem.c: 658 - 673

## Overview
A helper function that fetches a statistics entry reference and acquires a lock on it, providing thread-safe access to shared statistics data.

## Definition

```c
PgStat_EntryRef *
pgstat_get_entry_ref_locked(PgStat_Kind kind, Oid dboid, Oid objoid,
							bool nowait)
```
## Detailed Description
This function combines two operations into one convenient call: finding a statistics entry reference and locking it for safe access. It first calls  to locate the shared statistics entry corresponding to the specified parameters, then attempts to acquire a lock on that entry using . The function provides an option for non-blocking lock acquisition through the  parameter.

The function is designed to ensure thread-safe access to PostgreSQL's shared statistics system by preventing concurrent modifications while a process is reading or updating statistics data.

## Parameters / Member Variables
- : The type of statistics entry (e.g., database, relation, function statistics)
- : Database OID for the statistics entry
- : Object OID for the statistics entry
- : If true, returns NULL immediately if the lock cannot be acquired; if false, waits for the lock

## Dependencies
- Functions called/Symbols referenced:
  -  - Finds and returns the statistics entry reference
  -  - Acquires a lock on the statistics entry
  -  - Enumeration type for statistics kinds
  -  - Structure representing a statistics entry reference

- Called from (representative examples):
  -  - Reports autovacuum activity statistics
  -  - Reports database checksum failures
  -  - Copies relation statistics data
  -  - Reports vacuum operation statistics
  -  - Reports analyze operation statistics
  -  - Reports replication slot statistics

## Notes and Other Information
- Returns NULL if the lock cannot be acquired (when  is true) or if the entry reference cannot be found
- The caller is responsible for releasing the lock when done with the statistics entry
- This function is part of PostgreSQL's shared memory statistics system introduced to improve performance and reduce contention
- Located in 