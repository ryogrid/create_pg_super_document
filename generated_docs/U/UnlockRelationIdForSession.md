# UnlockRelationIdForSession

## Location
src/backend/storage/lmgr/lmgr.c: 400 - 419

## Overview
UnlockRelationIdForSession releases a session-level lock on a relation that was previously acquired with LockRelationIdForSession.

## Definition
```c
void UnlockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
```

## Detailed Description
This function releases a session-level lock on the specified relation. It constructs a relation lock tag from the LockRelId structure and calls LockRelease with the session flag set to true. This function is the counterpart to LockRelationIdForSession and should be used to explicitly release session locks when they are no longer needed, rather than waiting for backend exit or error conditions to clean them up.

## Parameters / Member Variables
- `relid`: Pointer to LockRelId structure containing database ID and relation ID
- `lockmode`: The lock mode to release on the relation (must match the originally acquired mode)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to construct relation lock tag)
  - LockRelease (performs the actual lock release with session=true)
- Called from (representative examples):
  - index_drop (when finishing index drop operations)
  - DefineIndex (when completing index creation)
  - vacuum_rel (when finishing vacuum operations)

## Notes and Other Information
- Must be paired with LockRelationIdForSession calls
- The lockmode parameter must match the mode used in the original lock acquisition
- Uses LockRelease with session=true to indicate session-level lock release
- Provides explicit control over session lock cleanup instead of relying on error or exit cleanup
- Located in src/backend/storage/lmgr/lmgr.c:400-419