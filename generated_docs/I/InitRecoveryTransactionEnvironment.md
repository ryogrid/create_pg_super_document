# InitRecoveryTransactionEnvironment

## Location
[src/backend/storage/ipc/standby.c:94-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L94-L159)

## Overview
Initializes tracking of the primary server's in-progress transactions during recovery, setting up hash tables for lock tracking and creating a virtual transaction for the Startup process.

## Definition
```c
void InitRecoveryTransactionEnvironment(void)
```

## Detailed Description
This function sets up the infrastructure needed for the Startup process to track and manage locks held by transactions on the primary server during recovery. It creates two hash tables: one for tracking individual locks (RecoveryLockHash) and another for tracking locks by transaction ID (RecoveryLockXidHash). The function also initializes shared invalidation management as a send-only process and creates a virtual transaction entry for the Startup process, allowing it to participate in the lock management system without being a full transaction.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (creates the recovery lock hash tables)
  - [SharedInvalBackendInit](../S/SharedInvalBackendInit.md) (initializes shared invalidation management)
  - GetNextLocalTransactionId (gets next local transaction ID)
  - VirtualXactLockTableInsert (inserts virtual transaction into lock table)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (main recovery process function)

## Notes and Other Information
- Must only be called once during startup (protected by Assert on RecoveryLockHash == NULL)
- Creates a permanent virtual transaction entry that remains throughout recovery
- The Startup process becomes visible in pg_locks after this function runs
- Sets up send-only shared invalidation to avoid reading messages or getting signaled on queue fill-up
- Uses hash tables with 64-bucket initial size for lock tracking
- Sets standbyState to STANDBY_INITIALIZED upon completion