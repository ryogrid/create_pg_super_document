# smgropen

## Location
src/backend/storage/smgr/smgr.c: 198 - 249

## Overview
Returns an SMgrRelation object for the specified relation, creating it if necessary, with transaction-scoped lifetime management.

## Definition
```c
SMgrRelation smgropen(RelFileLocator rlocator, ProcNumber backend)
```

## Detailed Description
The `smgropen` function is responsible for obtaining an SMgrRelation object that represents a relation in the storage manager system. Since PostgreSQL 17, this function provides improved lifetime management where the returned object remains valid for the lifetime of the current transaction until AtEOXact_SMgr() is called. For code running outside transactions, the object remains valid until explicitly destroyed. The function uses a hash table (`SMgrRelationHash`) to cache relation objects, creating new entries as needed. When creating a new relation object, it initializes various fields and performs implementation-specific initialization. Importantly, this function does not attempt to open the underlying physical files.

## Parameters / Member Variables
- `rlocator`: RelFileLocator specifying the tablespace, database, and relation number
- `backend`: ProcNumber identifying the backend process (for temporary relations)

## Dependencies
- Functions called/Symbols referenced:
  - RelFileNumberIsValid
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [dlist_init](../d/dlist_init.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - HASH_ELEM, HASH_BLOBS, HASH_ENTER
  - MAX_FORKNUM
  - SMgrRelation, RelFileLocatorBackend, SMgrRelationData, HASHCTL types
- Called from (representative examples):
  - XLogReadBufferExtended (src/backend/access/transam/xlogutils.c:491)
  - [RelationCreateStorage](../R/RelationCreateStorage.md) (src/backend/catalog/storage.c:149)
  - [ReadBufferWithoutRelcache](../R/ReadBufferWithoutRelcache.md) (src/backend/storage/buffer/bufmgr.c:833)
  - RelationGetSmgr (src/include/utils/rel.h:571)

## Notes and Other Information
- In PostgreSQL versions prior to 17, the object had no defined lifetime
- The function initializes a hash table on first use with a default size of 400 entries
- New relation objects are added to the unpinned_relns list with pincount = 0
- The smgr_which field is hardcoded to 0 (currently only md.c storage manager exists)
- Does not open physical files - that's handled by other functions
- Cached block numbers are initialized to InvalidBlockNumber