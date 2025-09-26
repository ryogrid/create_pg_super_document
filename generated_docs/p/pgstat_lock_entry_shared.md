# pgstat_lock_entry_shared

## Location
[src/backend/utils/activity/pgstat_shmem.c:637-648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L637-L648)

## Overview
Acquires a shared lock on a statistics entry for safe read-only access to its data.

## Definition
bool pgstat_lock_entry_shared(PgStat_EntryRef *entry_ref, bool nowait)

## Detailed Description
This function provides shared (read-only) locking for PostgreSQL statistics entries, allowing multiple concurrent readers while excluding writers. It operates on the same LWLock as pgstat_lock_entry() but acquires it in LW_SHARED mode instead of LW_EXCLUSIVE. When nowait is false, it blocks until the shared lock is acquired. When nowait is true, it attempts non-blocking acquisition and returns false if the lock cannot be immediately obtained. This is specifically separated from pgstat_lock_entry() because most callers need exclusive access, making the shared version a specialized case primarily used for read operations like fetching statistics.

## Parameters / Member Variables
- : Reference to the statistics entry to lock for reading
- : If true, return immediately if lock cannot be acquired; if false, wait for lock

## Dependencies
- Functions called/Symbols referenced:
  - LWLockConditionalAcquire
  - LWLockAcquire
- Called from (representative examples):
  - pgstat_fetch_entry

## Notes and Other Information
Separated from pgstat_lock_entry() as most callers need exclusive locks. The shared lock allows concurrent read access while preventing modifications. The function always returns true when nowait is false.