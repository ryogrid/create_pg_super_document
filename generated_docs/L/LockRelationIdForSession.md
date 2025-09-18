# LockRelationIdForSession

## Location
src/backend/storage/lmgr/lmgr.c: 387 - 399

## Overview
LockRelationIdForSession acquires a session-level lock on a relation that persists across transaction boundaries until explicitly released.

## Definition
```c
void LockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
```

## Detailed Description
This function grabs a session-level lock on the target relation specified by its LockRelId structure. Unlike transaction-level locks, session locks persist across transaction boundaries and will only be removed when UnlockRelationIdForSession() is called, when an ereport(ERROR) occurs, or when the backend exits. The function constructs a relation lock tag and calls LockAcquire with the session flag set to true.

## Parameters / Member Variables
- `relid`: Pointer to LockRelId structure containing database ID and relation ID
- `lockmode`: The lock mode to acquire on the relation

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to construct relation lock tag)
  - [LockAcquire](LockAcquire.md) (performs the actual lock acquisition with session=true, dontWait=false)
- Called from (representative examples):
  - [index_drop](../i/index_drop.md) (when dropping indexes)
  - [DefineIndex](../D/DefineIndex.md) (during index creation)
  - [vacuum_rel](../v/vacuum_rel.md) (during vacuum operations)

## Notes and Other Information
- [Session](../S/Session.md) locks persist across transaction boundaries unlike regular locks
- Should be paired with transaction-level locks in transactions that actually use the relation
- The session lock ensures relcache entry consistency across transactions
- Uses LockAcquire with session=true and dontWait=false parameters
- Located in src/backend/storage/lmgr/lmgr.c:387-399