# StandbyReleaseAllLocks

## Location
[src/backend/storage/ipc/standby.c:1105-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1105-L1125)

## Overview
StandbyReleaseAllLocks releases all AccessExclusiveLocks held by any transactions in the recovery lock hash table, typically called at the end of recovery or when a shutdown checkpoint is encountered.

## Definition

```c
void
StandbyReleaseAllLocks(void)
```
## Detailed Description
This function performs a complete cleanup of all locks stored in the RecoveryLockXidHash during standby recovery operations. It iterates through every entry in the hash table and releases all locks held by each transaction. This is a comprehensive lock release operation that ensures no recovery-related locks remain active.

The function is typically invoked in two scenarios: when recovery completes successfully, or when a shutdown checkpoint is processed during WAL replay. It uses hash table sequential scanning to systematically visit and clean up all lock entries.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - elog (DEBUG2 logging)
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table sequential scan)
  - [hash_seq_search](../h/hash_seq_search.md) (iterate through hash table entries)
  - [StandbyReleaseXidEntryLocks](StandbyReleaseXidEntryLocks.md) (release locks for individual transaction)
  - [hash_search](../h/hash_search.md) (remove entries from hash table with HASH_REMOVE)
- Data structures used:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [RecoveryLockXidEntry](../R/RecoveryLockXidEntry.md)
  - RecoveryLockXidHash
- Called from (representative examples):
  - [ShutdownRecoveryTransactionEnvironment](ShutdownRecoveryTransactionEnvironment.md) (src/backend/storage/ipc/standby.c:175)
  - [StandbyReleaseLocks](StandbyReleaseLocks.md) (src/backend/storage/ipc/standby.c:1080)

## Notes and Other Information
- This is a comprehensive cleanup function that releases ALL recovery locks
- Used during recovery shutdown and completion scenarios
- Includes DEBUG2 logging for troubleshooting lock release operations
- The function safely handles empty hash tables and continues until all entries are processed
- Located in src/backend/storage/ipc/standby.c:1105-1125