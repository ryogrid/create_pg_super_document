# pgstat_fetch_replslot

## Location
[src/backend/utils/activity/pgstat_replslot.c:170-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_replslot.c#L170-L188)

## Overview
Retrieves the statistics entry for a named replication slot, serving as a support function for SQL-callable pgstat functions.

## Definition
```c
PgStat_StatReplSlotEntry *pgstat_fetch_replslot(NameData slotname)
```

## Detailed Description
This function provides access to replication slot statistics for SQL-callable functions in PostgreSQL's statistics system. It takes a slot name as input and returns a pointer to the corresponding statistics entry if it exists. The function operates under a shared lock on the ReplicationSlotControlLock to safely access slot information. It first attempts to find the slot index by name, and if successful, retrieves the statistics entry from the shared statistics hash table. The function handles cases where the named slot doesn't exist by returning NULL.

## Parameters / Member Variables
- `slotname`: NameData structure containing the name of the replication slot whose statistics should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with ReplicationSlotControlLock, LW_SHARED)
  - [get_replslot_index](../g/get_replslot_index.md) (with NameStr conversion and create=false)
  - [pgstat_fetch_entry](pgstat_fetch_entry.md) (with PGSTAT_KIND_REPLSLOT)
  - [LWLockRelease](../L/LWLockRelease.md)
- Called from (representative examples):
  - PG_STAT_GET_REPLICATION_SLOT_COLS

## Notes and Other Information
- Returns NULL if the named replication slot doesn't exist
- Uses shared locking for safe concurrent access to slot information
- Designed specifically to support SQL-callable statistics functions
- Part of PostgreSQL's user-facing statistics interface
- The returned pointer should be used carefully as it points to shared memory
- Works with the get_replslot_index() function to translate names to internal indices