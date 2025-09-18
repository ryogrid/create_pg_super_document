# UnlockSharedObject

## Location
src/backend/storage/lmgr/lmgr.c: 1138 - 1158

## Overview
UnlockSharedObject releases a previously acquired lock on a shared database object identified by its class, object ID, and sub-object ID.

## Definition
```c
void UnlockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
```

## Detailed Description
This function releases a lock that was previously acquired on a shared database object. It constructs the appropriate lock tag using the provided object identifiers and calls the underlying lock release mechanism. This is the counterpart to LockSharedObject and ConditionalLockSharedObject functions, completing the lock/unlock cycle for shared objects in PostgreSQL.

## Parameters / Member Variables
- `classid`: Object identifier (OID) of the system catalog that contains the object to be unlocked
- `objid`: Object identifier (OID) of the specific object within the catalog to be unlocked  
- `objsubid`: Sub-object identifier for component parts of the object (e.g., column number for tables)
- `lockmode`: The type of lock to release (must match the lock mode that was originally acquired)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_OBJECT
  - LockRelease
- Types used:
  - LOCKTAG
- Called from (representative examples):
  - get_object_address
  - AddSubscriptionRelState
  - createdb_failure_callback
  - AlterDatabaseSet
  - get_db_info
  - replorigin_drop_by_name
  - drop_local_obsolete_slots
  - XLTW_Oper

## Notes and Other Information
- Must be called with the same lockmode that was used to acquire the lock
- Part of PostgreSQL's advisory locking system for shared objects
- Used in cleanup paths and normal unlock operations
- Does not return a value (void function)
- The function is located in src/backend/storage/lmgr/lmgr.c:1138-1158
- Commonly used in error cleanup scenarios and normal transaction completion