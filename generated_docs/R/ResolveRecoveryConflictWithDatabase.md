# ResolveRecoveryConflictWithDatabase

## Location
[src/backend/storage/ipc/standby.c:568-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L568-L621)

## Overview
This function forcibly disconnects all backend processes connected to a specific database during recovery, typically when the database is being dropped.

## Definition
```c
void ResolveRecoveryConflictWithDatabase(Oid dbid)
```

## Detailed Description
ResolveRecoveryConflictWithDatabase implements an immediate and aggressive approach to resolving recovery conflicts when a database needs to be removed during standby recovery. Unlike other recovery conflict resolution functions, it does not use the standard ResolveRecoveryConflictWithVirtualXIDs mechanism because that would wait for active transactions to complete, and completely idle sessions would block the database removal indefinitely.

The function employs a simple but effective strategy: it repeatedly counts the number of backends connected to the target database and forcibly cancels them all until no connections remain. This approach ensures that database dropping can proceed without being blocked by idle connections or long-running queries.

The implementation includes a safety mechanism - after each round of cancellations, it sleeps for a brief period (10ms) to avoid overwhelming unresponsive backends with cancellation signals, particularly when the system is under heavy load. This prevents resource exhaustion while still ensuring prompt conflict resolution.

The function assumes that AccessExclusiveLock has already been acquired on the database, which prevents new connections from being established during the cleanup process. Any processes attempting to connect during this period will block in InitPostgres() and subsequently disconnect when they discover the database no longer exists.

## Parameters / Member Variables
- `dbid`: Oid of the database for which all backend connections should be terminated

## Dependencies
- Functions called/Symbols referenced:
  - CountDBBackends
  - CancelDBBackends
  - PROCSIG_RECOVERY_CONFLICT_DATABASE
  - [pg_usleep](../p/pg_usleep.md)
- Called from (representative examples):
  - [dbase_redo](../d/dbase_redo.md)

## Notes and Other Information
- This function uses a fundamentally different approach from other recovery conflict resolution functions by not waiting for transaction completion
- The function operates under the assumption that AccessExclusiveLock is already held on the target database
- The 10ms sleep interval between cancellation attempts is designed to prevent flooding of unresponsive backends
- The function handles both active transactions and idle sessions by forcing immediate disconnection rather than graceful completion
- This aggressive approach is necessary because database dropping cannot proceed with any remaining connections
- New connection attempts during this process will be blocked by the lock and will fail when the database is removed
- The function is specifically designed for DROP DATABASE operations during WAL replay on standby servers
- The loop continues until CountDBBackends returns 0, ensuring complete cleanup before the database can be safely removed
- This represents one of the most disruptive types of recovery conflicts, as it forces immediate disconnection of all users of a database