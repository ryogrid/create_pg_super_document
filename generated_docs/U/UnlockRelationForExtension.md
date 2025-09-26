# UnlockRelationForExtension

## Location
[src/backend/storage/lmgr/lmgr.c:470-486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L470-L486)

## Overview
Releases a previously acquired extension lock on a relation, allowing other processes to proceed with relation extension operations.

## Definition

```c
void
UnlockRelationForExtension(Relation relation, LOCKMODE lockmode)
```
## Detailed Description
This function is the counterpart to LockRelationForExtension and ConditionalLockRelationForExtension. It releases an extension lock that was previously acquired on a relation, creating the same lock tag used during acquisition and calling the core lock release mechanism.

The function ensures that other processes waiting for the relation extension lock can proceed with their operations. It's essential to call this function after completing relation extension operations to prevent indefinite blocking of other processes and potential deadlock situations.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure representing the relation for which to release the extension lock
- `lockmode`: The type of lock to release, must match the lock mode used during acquisition

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION_EXTEND (macro to set up lock tag for relation extension)
  - LockRelease (core lock release function that handles lock cleanup and waiter notification)
- Called from (representative examples):
  - brin_page_cleanup (BRIN index page cleanup)
  - brin_getinsertbuffer (BRIN index buffer management, multiple call sites)
  - ginvacuumcleanup (GIN index vacuum cleanup)
  - gistvacuumscan (GiST index vacuum scan)
  - btvacuumscan (B-tree vacuum scan)
  - spgvacuumscan (SP-GiST vacuum scan)
  - ExtendBufferedRelTo (buffered relation extension)
  - ExtendBufferedRelShared (shared buffered relation extension, multiple call sites)
  - XLTW_Oper (transaction lock wait operations)

## Notes and Other Information
- Must be called with the exact same lockmode that was used to acquire the lock
- Failure to call this function after acquiring an extension lock will cause other processes to block indefinitely
- The function uses the same LOCKTAG_RELATION_EXTEND lock tag type as the acquisition functions
- Should be called in exception handling paths to ensure locks are released even when errors occur
- The underlying LockRelease function handles complex scenarios including fast-path locks and waiter notification
- Typically used in try-finally patterns or similar constructs to guarantee lock release