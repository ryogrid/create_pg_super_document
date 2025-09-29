# GetLockMethodLocalHash

## Location
[src/backend/storage/lmgr/lock.c:632-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L632-L642)

## Overview
A simple accessor function that returns the hash table containing all local locks held by the current backend, primarily used for assertion checking and debugging.

## Definition
HTAB *GetLockMethodLocalHash(void)

## Detailed Description
GetLockMethodLocalHash provides external access to the backend's private local lock hash table (LockMethodLocalHash). This function is designed specifically to support modules that need to evaluate assertions or perform debugging operations based on the complete set of locks held by the current backend.

The function simply returns a pointer to the LockMethodLocalHash, which contains LOCALLOCK entries representing all locks that the current backend has acquired or is interested in. This allows external code to iterate through or examine the backend's lock state without directly accessing the internal global variable.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - LockMethodLocalHash: The backend's local lock hash table (returned directly)
- Called from (representative examples):
  - [AssertPendingSyncs_RelationCache](../A/AssertPendingSyncs_RelationCache.md): Used in relation cache for assertion checking
  - LockHashPartitionLockByProc: Used in lock hash partition operations

## Notes and Other Information
- This is primarily a debugging and assertion support function
- Provides controlled access to internal lock state without exposing the global variable directly
- The returned hash table contains LOCALLOCK entries indexed by LOCALLOCKTAG
- Should be used carefully as it exposes internal lock manager state
- Useful for debugging lock-related issues and validating lock invariants
- The function has no side effects and simply returns a pointer to existing data
- External modules should treat the returned hash table as read-only to avoid corrupting lock state

## Simplified Source

```c
HTAB *GetLockMethodLocalHash(void)
{
    return LockMethodLocalHash;
}
```