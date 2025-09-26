# pgstat_unlock_entry

## Location
[src/backend/utils/activity/pgstat_shmem.c:649-657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L649-L657)

## Overview
Releases a lock on a statistics entry that was previously acquired through pgstat_lock_entry() or pgstat_lock_entry_shared().

## Definition
void pgstat_unlock_entry(PgStat_EntryRef *entry_ref)

## Detailed Description
This function releases locks on PostgreSQL statistics entries by calling LWLockRelease() on the embedded LWLock within the shared statistics structure. It works for both exclusive locks acquired via pgstat_lock_entry() and shared locks acquired via pgstat_lock_entry_shared(). The function is straightforward as LWLockRelease() automatically handles the appropriate unlock operation based on how the lock was originally acquired. This uniform unlock interface simplifies lock management for statistics entries.

## Parameters / Member Variables
- : Reference to the statistics entry to unlock

## Dependencies
- Functions called/Symbols referenced:
  - LWLockRelease
- Called from (representative examples):
  - pgstat_fetch_entry
  - pgstat_report_autovac
  - pgstat_report_checksum_failures_in_db
  - pgstat_reset_database_timestamp
  - pgstat_function_flush_cb
  - pgstat_copy_relation_stats
  - pgstat_report_vacuum
  - pgstat_report_analyze
  - pgstat_relation_flush_cb
  - pgstat_create_replslot
  - pgstat_reset_entry

## Notes and Other Information
Works uniformly for both exclusive and shared locks. LWLockRelease() automatically determines the correct unlock operation. Must be paired with a corresponding lock acquisition to maintain proper lock semantics.