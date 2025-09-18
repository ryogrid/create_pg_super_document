# UnlockSharedObject

## Location
[src/backend/storage/lmgr/lmgr.c:1138-1158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1138-L1158)

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
  - [LockRelease](../L/LockRelease.md)
- Types used:
  - LOCKTAG
- Called from (representative examples):
  - [get_object_address](../g/get_object_address.md)
  - [AddSubscriptionRelState](../A/AddSubscriptionRelState.md)
  - [createdb_failure_callback](../c/createdb_failure_callback.md)
  - [AlterDatabaseSet](../A/AlterDatabaseSet.md)
  - [get_db_info](../g/get_db_info.md)
  - [replorigin_drop_by_name](../r/replorigin_drop_by_name.md)
  - [drop_local_obsolete_slots](../d/drop_local_obsolete_slots.md)
  - [XLTW_Oper](../X/XLTW_Oper.md)

## Notes and Other Information
- Must be called with the same lockmode that was used to acquire the lock
- Part of PostgreSQL's advisory locking system for shared objects
- Used in cleanup paths and normal unlock operations
- Does not return a value (void function)
- The function is located in src/backend/storage/lmgr/lmgr.c:1138-1158
- Commonly used in error cleanup scenarios and normal transaction completion