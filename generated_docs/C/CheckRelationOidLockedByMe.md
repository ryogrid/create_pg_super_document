# CheckRelationOidLockedByMe

## Location
src/backend/storage/lmgr/lmgr.c: 347 - 362

## Overview
CheckRelationOidLockedByMe checks whether the current transaction holds a lock on a relation specified by its OID with the given lock mode or potentially stronger.

## Definition
```c
bool CheckRelationOidLockedByMe(Oid relid, LOCKMODE lockmode, bool orstronger)
```

## Detailed Description
This function is similar to CheckRelationLockedByMe but accepts a relation OID instead of a Relation structure. It constructs a lock tag using the relation OID via SetLocktagRelationOid, then delegates to LockHeldByMe to perform the actual lock ownership check. Like its counterpart, it can optionally check for stronger lock modes when the orstronger parameter is true.

## Parameters / Member Variables
- `relid`: The OID of the relation to check for lock ownership
- `lockmode`: The minimum lock mode to check for
- `orstronger`: If true, also accepts stronger (numerically higher) lock modes as satisfying the check

## Dependencies
- Functions called/Symbols referenced:
  - [SetLocktagRelationOid](../S/SetLocktagRelationOid.md) (constructs lock tag from relation OID)
  - [LockHeldByMe](../L/LockHeldByMe.md) (performs the actual lock ownership check)
- Called from (representative examples):
  - [UpdateSubscriptionRelStateEx](../U/UpdateSubscriptionRelStateEx.md)
  - [SetRelationHasSubclass](../S/SetRelationHasSubclass.md)

## Notes and Other Information
- Returns true if the current transaction holds the specified lock or stronger
- Simpler interface than CheckRelationLockedByMe when only the relation OID is available
- Uses SetLocktagRelationOid instead of SET_LOCKTAG_RELATION macro
- Located in src/backend/storage/lmgr/lmgr.c:347-362