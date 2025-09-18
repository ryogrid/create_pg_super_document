# SearchSysCacheLocked1

## Location
src/backend/utils/cache/syscache.c: 287 - 379

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
  - CatCache
  - LOCKTAG
  - ItemPointerSetInvalid
  - InplaceUpdateTupleLock
  - ItemPointerIsValid
  - LockRelease
  - ItemPointerEquals
  - SET_LOCKTAG_TUPLE
  - ItemPointerGetBlockNumber
  - ItemPointerGetOffsetNumber
  - LockAcquire
  - AcceptInvalidationMessages
- Called from (representative examples):
  - ExecGrant_Relation
  - ExecGrant_common
  - ATExecSetRelOptions
  - SearchSysCacheLockedCopy1

## Notes and Other Information
- Implements complex retry logic to handle race conditions with inplace updates
- The caller must call UnlockTuple(InplaceUpdateTupleLock) and ReleaseSysCache() after use
- Designed specifically for compliance with README.tuplock locking requirements
- The returned tuple may still be subject to uncommitted updates, so 'tuple concurrently updated' errors are still possible
- Uses a loop to ensure tuple TID stability between search and lock acquisition
- Processes invalidation messages to ensure cache consistency after lock acquisition
- Critical for maintaining data integrity when modifying inplace-updated catalog tables