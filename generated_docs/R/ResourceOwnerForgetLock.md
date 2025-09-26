# ResourceOwnerForgetLock

## Location
[src/backend/utils/resowner/resowner.c:1065-1084](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L1065-L1084)

## Overview
ResourceOwnerForgetLock removes a LOCALLOCK from a ResourceOwner's tracking cache, implementing efficient removal through backward linear search with swap-based deletion.

## Definition
```c
void ResourceOwnerForgetLock(ResourceOwner owner, LOCALLOCK *locallock)
```

## Detailed Description
ResourceOwnerForgetLock removes a specified LOCALLOCK from the ResourceOwner's lock cache. The function operates only when the cache hasn't overflowed (nlocks <= MAX_RESOWNER_LOCKS). It performs a backward linear search through the locks array to find the target lock, then removes it using an efficient swap-with-last-element technique to avoid shifting array elements.

The function handles three scenarios:
1. **Overflow state**: When nlocks > MAX_RESOWNER_LOCKS, it returns immediately as locks aren't being tracked
2. **Normal removal**: Searches backward through the array and swaps the found element with the last element
3. **Error case**: If the lock isn't found in the cache, it raises an ERROR

The backward search optimization takes advantage of the common pattern where recently added locks are more likely to be removed first.

## Parameters / Member Variables
- `owner`: The ResourceOwner from which to remove the lock tracking
- `locallock`: Pointer to the LOCALLOCK structure to be forgotten

## Dependencies
- Functions called/Symbols referenced:
  - MAX_RESOWNER_LOCKS (constant defining cache size limit)
  - [LOCALLOCK](../L/LOCALLOCK.md) (structure representing backend's view of a lock)
  - [ResourceOwner](ResourceOwner.md) (structure managing resource ownership)
  - elog (PostgreSQL logging/error reporting function)
- Called from (representative examples):
  - [RemoveLocalLock](RemoveLocalLock.md) (in src/backend/storage/lmgr/lock.c:1383)
  - [LockRelease](../L/LockRelease.md) (in src/backend/storage/lmgr/lock.c:2031)
  - [LockReleaseAll](../L/LockReleaseAll.md) (in src/backend/storage/lmgr/lock.c:2242)
  - [ReleaseLockIfHeld](ReleaseLockIfHeld.md) (in src/backend/storage/lmgr/lock.c:2538)
  - [LockReassignOwner](../L/LockReassignOwner.md) (in src/backend/storage/lmgr/lock.c:2637)

## Notes and Other Information
- The function uses backward iteration (i = nlocks-1 to 0) as an optimization, assuming recently added locks are more likely to be removed first
- Removal is implemented using swap-with-last-element technique to avoid expensive array element shifting
- When the cache has overflowed, the function becomes a no-op since locks aren't being tracked
- The function will raise an ERROR if the specified lock is not found in the cache, indicating a programming error
- The search is linear O(n) but limited to at most MAX_RESOWNER_LOCKS (15) iterations
- Each successful removal decrements nlocks, maintaining the cache size invariant