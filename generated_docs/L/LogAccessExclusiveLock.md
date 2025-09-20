# LogAccessExclusiveLock

## Location
[src/backend/storage/ipc/standby.c:1423-1439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1423-L1439)

## Overview
Logs an individual AccessExclusive lock acquisition to the Write-Ahead Log (WAL) for Hot Standby conflict resolution.

## Definition

```c
void
LogAccessExclusiveLock(Oid dbOid, Oid relOid)
```
## Detailed Description
LogAccessExclusiveLock records the acquisition of a single AccessExclusive lock during the LockAcquire() process. This function is called when an AccessExclusive lock is acquired on a relation to inform standby servers about potential conflicts with their read-only queries.

The function creates an xl_standby_lock record containing the current transaction ID, database OID, and relation OID, then delegates to LogAccessExclusiveLocks() to perform the actual WAL logging. It also sets the XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK flag in the current transaction's flags to track that this transaction has acquired AccessExclusive locks.

This logging is essential for Hot Standby operation, as AccessExclusive locks can conflict with read-only queries running on standby servers, requiring the standby to either wait or cancel conflicting queries.

## Parameters / Member Variables
- : The OID of the database containing the relation being locked
- : The OID of the relation (table, index, etc.) on which the AccessExclusive lock is being acquired

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [LogAccessExclusiveLocks](LogAccessExclusiveLocks.md)
  - XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK
  - [xl_standby_lock](../x/xl_standby_lock.md)
- Called from (representative examples):
  - [LockAcquireExtended](LockAcquireExtended.md)

## Notes and Other Information
- This function is specifically called during lock acquisition (LockAcquire()) to ensure real-time logging of conflicting locks
- Sets transaction flags to track AccessExclusive lock acquisition for transaction cleanup purposes
- Only AccessExclusive locks are logged because other lock modes do not conflict with read-only queries on standby servers
- The function is part of the Hot Standby infrastructure that enables read-only queries on standby servers
- Uses the bulk logging function LogAccessExclusiveLocks() even for single locks to maintain consistency
- Located in src/backend/storage/ipc/standby.c:1423-1439