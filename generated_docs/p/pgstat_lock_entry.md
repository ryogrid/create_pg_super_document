# pgstat_lock_entry

## Location
src/backend/utils/activity/pgstat_shmem.c: 621 - 636

## Overview
Acquires an exclusive lock on a statistics entry for safe modification of its data.

## Definition
bool pgstat_lock_entry(PgStat_EntryRef *entry_ref, bool nowait)

## Detailed Description
This function provides exclusive locking for PostgreSQL statistics entries to ensure thread-safe access during modifications. It operates on the LWLock embedded within the shared statistics structure. When nowait is false, it blocks until the lock is acquired using LWLockAcquire(). When nowait is true, it attempts non-blocking acquisition using LWLockConditionalAcquire() and returns false if the lock cannot be immediately obtained. This exclusive locking is essential for operations that modify statistics data, such as flushing pending statistics or resetting counters.

## Parameters / Member Variables
- : Reference to the statistics entry to lock
- : If true, return immediately if lock cannot be acquired; if false, wait for lock

## Dependencies
- Functions called/Symbols referenced:
  - LWLockConditionalAcquire
  - LWLockAcquire
- Called from (representative examples):
  - pgstat_database_flush_cb
  - pgstat_function_flush_cb
  - pgstat_relation_flush_cb
  - pgstat_get_entry_ref_locked
  - pgstat_reset_entry

## Notes and Other Information
Most statistics operations require exclusive access, making this the primary locking function. The function always returns true when nowait is false, as LWLockAcquire() will block until successful.