# CountDBBackends

## Location
[src/backend/storage/ipc/procarray.c:3598-3626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3598-L3626)

## Overview
Counts the number of backends that are currently using a specified database, providing a way to determine database usage for administrative and recovery conflict resolution purposes.

## Definition
```c
int CountDBBackends(Oid databaseid)
```

## Detailed Description
CountDBBackends iterates through the process array to count active backends connected to a specific database. Unlike MinimumActiveBackends, this function acquires the ProcArrayLock in shared mode to ensure consistent results, as it's used for more critical operations like recovery conflict resolution.

The function filters out prepared transactions (backends with pid == 0) since they don't represent active user connections. If an invalid database OID is passed, the function counts all backends regardless of their database connection.

This function is particularly important in standby servers during recovery, where it helps determine if there are active connections to a database that might conflict with recovery operations.

## Parameters / Member Variables
- `databaseid`: The OID of the database for which to count backends. If InvalidOid is passed, counts all backends.

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (procArray global variable)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for ProcArrayLock in LW_SHARED mode)
  - [PGPROC](../P/PGPROC.md) (process structure)
  - OidIsValid (macro to check if OID is valid)

- Called from (representative examples):
  - [ResolveRecoveryConflictWithDatabase](../R/ResolveRecoveryConflictWithDatabase.md) (in src/backend/storage/ipc/standby.c:581)

## Notes and Other Information
- This function properly acquires ProcArrayLock, making it safe for use in critical operations
- Prepared transactions are excluded from the count as they don't represent active connections
- The function supports counting all backends by passing an invalid database OID
- Primarily used in standby server scenarios for managing recovery conflicts
- The lock acquisition ensures atomicity and consistency of the count, unlike the heuristic MinimumActiveBackends function