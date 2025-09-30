# SearchSysCacheLocked1

## Location
[src/backend/utils/cache/syscache.c:287-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L287-L379)

## Overview
SearchSysCacheLocked1 combines system cache lookup with tuple-level locking to safely handle inplace-updated system catalog tables.

## Definition
HeapTuple SearchSysCacheLocked1(int cacheId, Datum key1)

## Detailed Description
SearchSysCacheLocked1 is a specialized function that combines SearchSysCache1() with acquiring a LOCKTAG_TUPLE lock in InplaceUpdateTupleLock mode. It is designed to handle concurrency issues with inplace-updated system catalog tables by ensuring the returned tuple is locked against concurrent modifications. The function implements a retry loop to handle cases where the tuple's TID changes between search and lock acquisition, ensuring consistency in the face of concurrent inplace updates.

## Parameters / Member Variables
- cacheId: Integer identifier of the system cache to search in
- key1: Search key value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [CatCache](../C/CatCache.md)
  - [LOCKTAG](../L/LOCKTAG.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - InplaceUpdateTupleLock
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [LockRelease](../L/LockRelease.md)
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - SET_LOCKTAG_TUPLE
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [LockAcquire](../L/LockAcquire.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
- Called from (representative examples):
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md)
  - [ExecGrant_common](../E/ExecGrant_common.md)
  - [ATExecSetRelOptions](../A/ATExecSetRelOptions.md)
  - [SearchSysCacheLockedCopy1](SearchSysCacheLockedCopy1.md)

## Notes and Other Information
- Implements complex retry logic to handle race conditions with inplace updates
- The caller must call UnlockTuple(InplaceUpdateTupleLock) and ReleaseSysCache() after use
- Designed specifically for compliance with README.tuplock locking requirements
- The returned tuple may still be subject to uncommitted updates, so 'tuple concurrently updated' errors are still possible
- Uses a loop to ensure tuple TID stability between search and lock acquisition
- Processes invalidation messages to ensure cache consistency after lock acquisition
- Critical for maintaining data integrity when modifying inplace-updated catalog tables

## Simplified Source

```c
HeapTuple SearchSysCacheLocked1(int cacheId, Datum key1) {
    CatCache *cache = SysCache[cacheId];
    ItemPointerData tid;
    LOCKTAG tag;

    ItemPointerSetInvalid(&tid);

    // Retry loop to handle TID changes during concurrent updates
    for (;;) {
        HeapTuple tuple = SearchSysCache1(cacheId, key1);
        LOCKMODE lockmode = InplaceUpdateTupleLock;

        // If we already have a lock, check if TID matches
        if (ItemPointerIsValid(&tid)) {
            if (!HeapTupleIsValid(tuple)) {
                LockRelease(&tag, lockmode, false);
                return tuple;
            }
            if (ItemPointerEquals(&tid, &tuple->t_self)) {
                return tuple;  // Same tuple, return it
            }
            LockRelease(&tag, lockmode, false);  // TID changed, release old lock
        } else if (!HeapTupleIsValid(tuple)) {
            return tuple;  // No tuple found
        }

        // Remember new TID and release tuple
        tid = tuple->t_self;
        ReleaseSysCache(tuple);

        // Acquire lock on the tuple
        SET_LOCKTAG_TUPLE(tag,
                         cache->cc_relisshared ? InvalidOid : MyDatabaseId,
                         cache->cc_reloid,
                         ItemPointerGetBlockNumber(&tid),
                         ItemPointerGetOffsetNumber(&tid));
        LockAcquire(&tag, lockmode, false, false);

        // Process any pending invalidation messages
        AcceptInvalidationMessages();

        // Loop back to verify TID is still current
    }
}
```