# dshash_seq_term

## Location
src/backend/lib/dshash.c: 747 - 756

## Overview
Terminates a sequential scan of a dynamic shared hash table and releases all partition locks held during the scan.

## Definition
```c
void dshash_seq_term(dshash_seq_status *status)
```

## Detailed Description
dshash_seq_term provides cleanup functionality for sequential scans of dynamic shared hash tables. The function releases any partition locks that were acquired during the scan process, ensuring proper resource cleanup and allowing other processes to access the hash table. It checks if a partition is currently locked before attempting to release the lock, making it safe to call even if the scan was terminated early or never started.

## Parameters / Member Variables
- `status`: Pointer to the scan status structure that tracks the current scan state, including which partition (if any) is currently locked

## Dependencies
- Functions called/Symbols referenced:
  - dshash_seq_status (scan status structure type)
  - PARTITION_LOCK (partition lock macro)
  - LWLockRelease (lock release function)
- Called from (representative examples):
  - pgstat_build_snapshot (src/backend/utils/activity/pgstat.c:1044)
  - pgstat_write_statsfile (src/backend/utils/activity/pgstat.c:1441)
  - pgstat_drop_database_and_contents (src/backend/utils/activity/pgstat_shmem.c:905)
  - pgstat_drop_all_entries (src/backend/utils/activity/pgstat_shmem.c:986)
  - pgstat_reset_matching_entries (src/backend/utils/activity/pgstat_shmem.c:1055)

## Notes and Other Information
- Must be called to properly terminate any scan initiated with dshash_seq_init()
- Safe to call even if no locks are held (curpartition < 0)
- Essential for preventing lock leaks and ensuring other processes can access the hash table
- Should be called both when a scan completes normally and when it's terminated early due to errors
- Primarily used in PostgreSQL's statistics system as part of the cleanup process for statistics data iteration
- The function is idempotent - calling it multiple times on the same status structure is safe