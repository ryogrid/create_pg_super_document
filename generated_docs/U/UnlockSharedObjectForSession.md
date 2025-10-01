# UnlockSharedObjectForSession

## Location
[src/backend/storage/lmgr/lmgr.c:1177-1198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1177-L1198)

## Overview
UnlockSharedObjectForSession releases a session-level lock that was previously acquired on a shared database object using LockSharedObjectForSession.

## Definition
```c
void UnlockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
```

## Detailed Description
This function releases a session-level lock on a shared database object. It is the counterpart to LockSharedObjectForSession and must be called to explicitly release session-level locks, as they do not automatically release at transaction end like regular locks. The function constructs the appropriate lock tag and calls the underlying lock release mechanism with the session=true parameter.

## Parameters / Member Variables
- `classid`: Object identifier (OID) of the system catalog that contains the object to be unlocked
- `objid`: Object identifier (OID) of the specific object within the catalog to be unlocked  
- `objsubid`: Sub-object identifier for component parts of the object (e.g., column number for tables)
- `lockmode`: The type of lock to release (must match the lock mode that was originally acquired)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_OBJECT
  - [LockRelease](../L/LockRelease.md)
- Types used:
  - [LOCKTAG](../L/LOCKTAG.md)
- Called from (representative examples):
  - [movedb](../m/movedb.md)
  - [dbase_redo](../d/dbase_redo.md)
  - [XLTW_Oper](../X/XLTW_Oper.md)

## Notes and Other Information
- Releases session-level locks that were acquired with LockSharedObjectForSession
- Uses session=true parameter in LockRelease call to indicate session-level unlock
- Must be called with the same lockmode that was used to acquire the session lock
- Part of PostgreSQL's advisory locking system for shared objects
- Essential for proper cleanup of session-level locks
- The function is located in src/backend/storage/lmgr/lmgr.c:1177-1198
- Does not return a value (void function)
- Used in database operations that require cross-transaction resource management

## Simplified Source

```c
void
UnlockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid,
                            LOCKMODE lockmode)
{
    LOCKTAG tag;

    // Construct lock tag for the shared object
    SET_LOCKTAG_OBJECT(tag,
                      InvalidOid,
                      classid,
                      objid,
                      objsubid);

    // Release the session-level lock
    LockRelease(&tag, lockmode, true);
}
```