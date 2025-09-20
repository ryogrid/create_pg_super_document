# replorigin_session_reset

## Location
[src/backend/replication/logical/origin.c:1190-1218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1190-L1218)

## Overview
Resets and clears the replication origin session state that was previously established with replorigin_session_setup(), releasing the origin slot for other processes to use.

## Definition

```c
void
replorigin_session_reset(void)
```
## Detailed Description
This function tears down the current replication origin session by clearing the session state and releasing the acquired slot. It performs the inverse operation of replorigin_session_setup() by:

1. Validating that a replication origin session is currently active
2. Acquiring exclusive lock on the ReplicationOriginLock
3. Marking the slot as not acquired (acquired_by = 0)
4. Clearing the session_replication_state pointer
5. Broadcasting to any waiting processes via condition variable
6. Releasing the lock

The function ensures proper cleanup and allows other processes to acquire the same origin slot if needed.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (ReplicationOriginLock)
  - ConditionVariableBroadcast
  - ereport
- Called from (representative examples):
  - [pg_replication_origin_session_reset](../p/pg_replication_origin_session_reset.md)
  - [process_syncing_tables_for_sync](../p/process_syncing_tables_for_sync.md)

## Notes and Other Information
- Must only be called if a replication origin was previously setup with replorigin_session_setup()
- Throws error if no replication origin is currently configured
- Uses exclusive locking to ensure thread-safe state changes
- Broadcasts condition variable signal to wake up any processes waiting for this origin slot
- Required for proper resource cleanup when switching between different replication origins