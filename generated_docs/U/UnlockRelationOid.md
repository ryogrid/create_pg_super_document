# UnlockRelationOid

## Location
src/backend/storage/lmgr/lmgr.c: 227 - 243

## Overview
UnlockRelationOid releases a lock on a relation using only the relation's OID, though UnlockRelationId is preferred when available for better performance.

## Definition
```c
void UnlockRelationOid(Oid relid, LOCKMODE lockmode)
```

## Detailed Description
This function unlocks a relation lock identified by a relation OID and lock mode. It uses SetLocktagRelationOid to create a LOCKTAG for the relation from the OID, then calls LockRelease to perform the actual unlock operation. While functional, this method is less efficient than UnlockRelationId because it requires looking up the database ID internally to create the complete lock tag, whereas UnlockRelationId already has both database ID and relation ID available.

## Parameters / Member Variables
- `relid`: The OID of the relation to unlock
- `lockmode`: The lock mode to release (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SetLocktagRelationOid (sets up lock tag from relation OID)
  - LockRelease (performs the actual lock release)
- Types referenced:
  - Oid (object identifier type)
  - LOCKTAG (lock tag structure)
  - LOCKMODE (enumeration of lock modes)
- Called from (representative examples):
  - try_relation_open (in relation.c:106)
  - ReleaseDeletionLock (in dependency.c:1531)
  - RangeVarGetRelidExtended (in namespace.c:579)
  - LockTableRecurse (in lockcmds.c:154)
  - do_autovacuum (in autovacuum.c:2174, 2188, 2194, 2212)
  - AcquireExecutorLocks (in plancache.c:1814)

## Notes and Other Information
- Less efficient than UnlockRelationId due to OID-to-LockRelId conversion overhead
- Used when only the relation OID is available and LockRelId is not accessible
- The function sets sessionLock parameter to false in LockRelease, indicating this is not a session-level lock
- Commonly used in catalog operations, namespace lookups, and autovacuum processes
- Part of PostgreSQL's lock manager subsystem located in src/backend/storage/lmgr/lmgr.c
- Widely used throughout the codebase due to the prevalence of OID-based relation identification