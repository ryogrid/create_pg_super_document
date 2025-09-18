# ShutdownRecoveryTransactionEnvironment

## Location
src/backend/storage/ipc/standby.c: 160 - 199

## Overview
Shuts down transaction tracking infrastructure during recovery, releasing all held locks and destroying hash tables when transitioning from hot standby to normal operation.

## Definition
```c
void ShutdownRecoveryTransactionEnvironment(void)
```

## Detailed Description
This function performs cleanup of the recovery transaction tracking environment established by InitRecoveryTransactionEnvironment. It safely shuts down all recovery-time transaction tracking by marking in-progress transactions as finished, releasing all locks held by tracked transactions, destroying the lock hash tables, and cleaning up the virtual transaction. The function is designed to be safe for redundant calls during process exit, as it checks if the environment has already been shut down.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ExpireAllKnownAssignedTransactionIds (marks all tracked transactions as finished)
  - StandbyReleaseAllLocks (releases all locks held by tracked transactions)
  - [hash_destroy](../h/hash_destroy.md) (destroys the recovery lock hash tables)
  - VirtualXactLockTableCleanup (cleans up the virtual transaction entry)
- Called from (representative examples):
  - [StartupXLOG](StartupXLOG.md) (main recovery process function)
  - [StartupProcExit](StartupProcExit.md) (startup process exit handler)

## Notes and Other Information
- Safe to call multiple times - checks if RecoveryLockHash is NULL to avoid redundant operations
- Must be called during startup process shutdown to prevent lock leaks that could interfere with other processes
- Critical for proper transition from hot standby mode to normal operation
- Performs cleanup in logical order: expire transactions, release locks, destroy hash tables, cleanup virtual transaction
- Sets RecoveryLockHash and RecoveryLockXidHash to NULL after destruction for safety