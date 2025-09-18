# LockApplyTransactionForSession

## Location
src/backend/storage/lmgr/lmgr.c: 1199 - 1216

## Overview
LockApplyTransactionForSession obtains a session-level lock on a transaction being applied on a logical replication subscriber, ensuring coordination between parallel apply workers.

## Definition
```c
void LockApplyTransactionForSession(Oid suboid, TransactionId xid, uint16 objid, LOCKMODE lockmode)
```

## Detailed Description
This function acquires a session-level lock specifically for logical replication apply transactions. It is used to coordinate access to transactions being applied by logical replication workers, particularly in parallel apply scenarios where multiple workers might need to synchronize their work on the same or related transactions. The lock persists for the duration of the session rather than just the current transaction.

## Parameters / Member Variables
- `suboid`: Object identifier (OID) of the subscription that owns the apply transaction
- `xid`: Transaction identifier of the transaction being applied from the publisher
- `objid`: Additional object identifier for finer-grained locking (typically sub-transaction or object-specific)
- `lockmode`: The type of lock to acquire (from LOCKMODE enum, e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_APPLY_TRANSACTION
  - [LockAcquire](LockAcquire.md)
- Types used:
  - LOCKTAG
  - TransactionId
- Global variables used:
  - MyDatabaseId
- Called from (representative examples):
  - [pa_lock_stream](../p/pa_lock_stream.md)
  - [pa_lock_transaction](../p/pa_lock_transaction.md)
  - [XLTW_Oper](../X/XLTW_Oper.md)

## Notes and Other Information
- Specialized for logical replication apply worker coordination
- Uses SET_LOCKTAG_APPLY_TRANSACTION to create transaction-specific lock tags
- Acquires session-level locks that persist until session end
- Part of PostgreSQL's parallel apply worker synchronization mechanism
- Essential for preventing conflicts between parallel logical replication workers
- The function is located in src/backend/storage/lmgr/lmgr.c:1199-1216
- Does not return a value (void function)
- Includes MyDatabaseId in the lock tag to scope locks to the current database
- Used primarily in logical replication parallel apply worker scenarios