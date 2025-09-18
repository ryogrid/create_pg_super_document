# SearchSysCacheLockedCopy1

## Location
[src/backend/utils/cache/syscache.c:405-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L405-L425)

## Overview
SearchSysCacheLockedCopy1 is a system cache function that searches for a tuple by a single key and returns a copy of the tuple while handling proper locking semantics for in-place updates.

## Definition
```c
HeapTuple SearchSysCacheLockedCopy1(int cacheId, Datum key1)
```

## Detailed Description
This function combines the functionality of SearchSysCacheLocked1 and SearchSysCacheCopy. It searches the system cache for a tuple matching the provided key, creates a copy of the found tuple, and releases the original cached tuple. The function is specifically designed for scenarios where the caller intends to perform heap_update() operations on the returned tuple. After such operations, the caller should call UnlockTuple(InplaceUpdateTupleLock) and heap_freetuple() to properly clean up resources.

The function first searches for the tuple using SearchSysCacheLocked1, which returns a locked reference to the cached tuple. If a valid tuple is found, it creates a copy using heap_copytuple, releases the original cached tuple, and returns the copy. This approach ensures that the original cached tuple remains available for other concurrent operations while providing the caller with a modifiable copy.

## Parameters / Member Variables
- `cacheId`: The identifier of the system cache to search in (corresponds to entries in SysCacheIdentifier enum)
- `key1`: The search key value used to locate the desired tuple in the cache

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheLocked1](SearchSysCacheLocked1.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - HeapTupleIsValid
- Called from (representative examples):
  - [get_catalog_object_by_oid_extended](../g/get_catalog_object_by_oid_extended.md)
  - [RenameDatabase](../R/RenameDatabase.md)
  - [SetDatabaseHasLoginEventTriggers](SetDatabaseHasLoginEventTriggers.md)
  - [update_relispartition](../u/update_relispartition.md)
  - [SetRelationTableSpace](SetRelationTableSpace.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [AlterRelationNamespaceInternal](../A/AlterRelationNamespaceInternal.md)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md)

## Notes and Other Information
- This function is part of PostgreSQL's system cache infrastructure, which provides fast access to frequently used catalog information
- The function returns a copy of the tuple rather than a reference to the cached version, allowing the caller to modify the tuple without affecting the cache
- Proper cleanup is essential: after using the returned tuple for heap_update(), callers must call UnlockTuple(InplaceUpdateTupleLock) and heap_freetuple()
- The function returns an invalid HeapTuple if no matching entry is found in the cache
- This function is typically used in DDL operations where catalog tuples need to be modified