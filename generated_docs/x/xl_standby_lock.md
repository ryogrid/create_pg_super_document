# xl_standby_lock

## Location
src/include/storage/lockdefs.h: 54 - 59

## Overview
A WAL (Write-Ahead Log) record structure that represents an AccessExclusiveLock on a table for standby server lock conflict resolution.

## Definition


## Detailed Description
The  structure is used in PostgreSQL's WAL system to record information about AccessExclusiveLocks on tables. This information is crucial for standby servers in hot standby mode to properly handle lock conflicts and maintain consistency with the primary server. When a transaction acquires an AccessExclusiveLock on a table, this structure captures the essential details needed for standby servers to understand the locking state and resolve potential conflicts with running queries on the standby.

## Parameters / Member Variables
- : The transaction ID of the transaction that holds the AccessExclusiveLock
- : The object identifier of the database containing the locked table
- : The object identifier of the table that is locked

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (type)
  - Oid (type)
- Called from (representative examples):
  - [LogAccessExclusiveLock](../L/LogAccessExclusiveLock.md) (src/backend/storage/ipc/standby.c:1425)
  - [LogAccessExclusiveLocks](../L/LogAccessExclusiveLocks.md) (src/backend/storage/ipc/standby.c:1405, 1413)
  - [StandbyAcquireAccessExclusiveLock](../S/StandbyAcquireAccessExclusiveLock.md) (src/backend/storage/ipc/standby.c:989)
  - [GetRunningTransactionLocks](../G/GetRunningTransactionLocks.md) (src/backend/storage/lmgr/lock.c:3990, 4012)

## Notes and Other Information
- This structure is part of PostgreSQL's hot standby functionality, enabling read-only queries on standby servers
- The structure is logged to WAL to inform standby servers about exclusive locks that might conflict with running queries
- Used in conjunction with standby lock conflict resolution mechanisms to maintain data consistency
- Related to the xl_standby_locks structure which contains an array of these lock records
- Defined in src/include/storage/lockdefs.h:54-59