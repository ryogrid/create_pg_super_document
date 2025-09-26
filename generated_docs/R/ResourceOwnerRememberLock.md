# ResourceOwnerRememberLock

## Location
src/backend/utils/resowner/resowner.c: 1045 - 1064

## Overview
ResourceOwnerRememberLock registers a LOCALLOCK with a ResourceOwner to track lock ownership, implementing a lossy cache mechanism that can hold up to 15 locks before overflowing.

## Definition
```c
void ResourceOwnerRememberLock(ResourceOwner owner, LOCALLOCK *locallock)
```

## Detailed Description
ResourceOwnerRememberLock maintains a cache of locks owned by a ResourceOwner. Unlike generic resource tracking, this implementation uses a lossy cache design with a maximum capacity of MAX_RESOWNER_LOCKS (15) entries. When the cache overflows, the function stops tracking additional locks but continues to increment the count. This design optimizes performance by preventing ResourceOwnerForgetLock from scanning through large arrays when many locks are held.

The function operates in three states:
1. **Normal operation**: When nlocks < MAX_RESOWNER_LOCKS, it adds the lock to the array
2. **Overflow point**: When nlocks == MAX_RESOWNER_LOCKS, it increments the count but doesn't store the lock
3. **Post-overflow**: When nlocks > MAX_RESOWNER_LOCKS, it simply increments the count and returns immediately

## Parameters / Member Variables
- `owner`: The ResourceOwner that will track the lock
- `locallock`: Pointer to the LOCALLOCK structure to be remembered (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - MAX_RESOWNER_LOCKS (constant defining cache size limit)
  - LOCALLOCK (structure representing backend's view of a lock)
  - ResourceOwner (structure managing resource ownership)
- Called from (representative examples):
  - GrantLockLocal (in src/backend/storage/lmgr/lock.c:1713)
  - LockReassignOwner (in src/backend/storage/lmgr/lock.c:2626)

## Notes and Other Information
- The lossy cache design is a performance optimization to prevent expensive linear searches in ResourceOwnerForgetLock when many locks are held
- MAX_RESOWNER_LOCKS is set to 15 based on testing with pg_dump, which showed most resource owners need fewer than 9 locks
- The function includes an assertion that locallock is not NULL but does not validate the ResourceOwner
- Once overflow occurs, the function becomes very lightweight, only incrementing a counter