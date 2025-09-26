# LockSharedObjectForSession

## Location
[src/backend/storage/lmgr/lmgr.c:1159-1176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1159-L1176)

## Overview
LockSharedObjectForSession obtains a session-level lock on a shared database object that persists until the end of the session rather than the end of the transaction.

## Definition
```c
void LockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
```

## Detailed Description
This function acquires a session-level lock on a shared database object, similar to LockSharedObject but with session-level persistence. Session-level locks are held until the session ends, unlike regular transaction-level locks that are released at transaction commit or abort. This is particularly useful for operations that need to maintain exclusive access to resources across multiple transactions within the same session.

## Parameters / Member Variables
- `classid`: Object identifier (OID) of the system catalog that contains the object to be locked
- `objid`: Object identifier (OID) of the specific object within the catalog to be locked  
- `objsubid`: Sub-object identifier for component parts of the object (e.g., column number for tables)
- `lockmode`: The type of lock to acquire (from LOCKMODE enum, e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_OBJECT
  - [LockAcquire](LockAcquire.md)
- Types used:
  - [LOCKTAG](LOCKTAG.md)
- Called from (representative examples):
  - [movedb](../m/movedb.md)
  - [dbase_redo](../d/dbase_redo.md)
  - [XLTW_Oper](../X/XLTW_Oper.md)

## Notes and Other Information
- Acquires session-level locks that persist until session end, not transaction end
- Uses session=true parameter in LockAcquire call to indicate session-level locking
- Referenced in LockRelationIdForSession documentation for session-level lock behavior
- Part of PostgreSQL's advisory locking system for shared objects
- Commonly used in operations that require cross-transaction resource protection
- The function is located in src/backend/storage/lmgr/lmgr.c:1159-1176
- Does not return a value (void function)
- Must be paired with UnlockSharedObjectForSession to release the lock