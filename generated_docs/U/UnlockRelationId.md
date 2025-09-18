# UnlockRelationId

## Location
src/backend/storage/lmgr/lmgr.c: 212 - 226

## Overview
UnlockRelationId releases a lock on a relation using a LockRelId structure, which is the preferred method over UnlockRelationOid for performance reasons.

## Definition
```c
void UnlockRelationId(LockRelId *relid, LOCKMODE lockmode)
```

## Detailed Description
This function unlocks a relation lock identified by a LockRelId structure and lock mode. It creates a LOCKTAG for the relation using the database ID and relation ID from the LockRelId, then calls LockRelease to perform the actual unlock operation. This function is preferred over UnlockRelationOid because it avoids the overhead of OID-to-LockRelId conversion, making it faster when the LockRelId is already available.

## Parameters / Member Variables
- `relid`: Pointer to LockRelId structure containing database ID and relation ID identifying the relation to unlock
- `lockmode`: The lock mode to release (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to set up lock tag for relation)
  - LockRelease (performs the actual lock release)
- Types referenced:
  - LockRelId (structure containing database and relation IDs)
  - LOCKTAG (lock tag structure)
  - LOCKMODE (enumeration of lock modes)
- Called from (representative examples):
  - relation_close (in relation.c:215)
  - index_close (in indexam.c:187)
  - CreateDatabaseUsingWalLog (in dbcommands.c:219, 220)
  - ScanSourceDatabasePgClass (in dbcommands.c:318)

## Notes and Other Information
- This is the preferred unlocking method when a LockRelId is available, as it avoids OID lookup overhead
- The function sets sessionLock parameter to false in LockRelease, indicating this is not a session-level lock
- Part of PostgreSQL's lock manager subsystem located in src/backend/storage/lmgr/lmgr.c
- Works in conjunction with LockRelationId to provide relation-level locking