# LockReleaseSession

## Location
[src/backend/storage/lmgr/lock.c:2444-2473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2444-L2473)

## Overview
LockReleaseSession releases all session locks of a specified lock method that are held by the current process.

## Definition

```c
void
LockReleaseSession(LOCKMETHODID lockmethodid)
```
## Detailed Description
This function iterates through the local lock hash table and releases all session locks held by the current process that belong to the specified lock method. It uses a hash sequence scan to traverse all LOCALLOCK entries, filtering for those matching the specified lock method ID, and calls ReleaseLockIfHeld to release each qualifying lock. The function validates that the provided lock method ID is within valid bounds before proceeding.

## Parameters / Member Variables
- `lockmethodid`: The ID of the lock method whose session locks should be released. Must be a valid lock method identifier within the range of defined lock methods.
## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md): Initializes sequential hash table scanning
  - [hash_seq_search](../h/hash_seq_search.md): Gets next entry during hash table traversal  
  - LOCALLOCK_LOCKMETHOD: Macro to extract lock method from LOCALLOCK
  - [ReleaseLockIfHeld](../R/ReleaseLockIfHeld.md): Releases a specific lock if it is held
  - lengthof: Macro to get array length
  - elog: Error logging function
- Called from (representative examples):
  - [pg_advisory_unlock_all](../p/pg_advisory_unlock_all.md): Used to unlock all advisory locks
  - LockHashPartitionLockByProc: Referenced in lock management header

## Notes and Other Information
- This function is specifically designed for session cleanup, ensuring that all locks acquired during a session are properly released
- The function performs bounds checking on the lock method ID and will throw an error for invalid values
- Only processes locks that match the specified lock method, ignoring others during the hash table scan
- The session parameter (true) passed to ReleaseLockIfHeld indicates these are session-level locks rather than transaction-level locks