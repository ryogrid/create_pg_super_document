# smgropen

## Location
[src/backend/storage/smgr/smgr.c:198-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L198-L249)

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
  - [XLogReadBufferExtended](../X/XLogReadBufferExtended.md) (src/backend/access/transam/xlogutils.c:491)
  - [RelationCreateStorage](../R/RelationCreateStorage.md) (src/backend/catalog/storage.c:149)
  - [ReadBufferWithoutRelcache](../R/ReadBufferWithoutRelcache.md) (src/backend/storage/buffer/bufmgr.c:833)
  - [RelationGetSmgr](../R/RelationGetSmgr.md) (src/include/utils/rel.h:571)

## Notes and Other Information
- In PostgreSQL versions prior to 17, the object had no defined lifetime
- The function initializes a hash table on first use with a default size of 400 entries
- New relation objects are added to the unpinned_relns list with pincount = 0
- The smgr_which field is hardcoded to 0 (currently only md.c storage manager exists)
- Does not open physical files - that's handled by other functions
- Cached block numbers are initialized to InvalidBlockNumber

## Simplified Source

```c
// Simplified version of smgropen
SMgrRelation
smgropen(RelFileLocator rlocator, ProcNumber backend) {
    RelFileLocatorBackend brlocator;
    SMgrRelation reln;
    bool found;

    // Initialize hash table on first use
    if (SMgrRelationHash == NULL) {
        HASHCTL ctl;
        ctl.keysize = sizeof(RelFileLocatorBackend);
        ctl.entrysize = sizeof(SMgrRelationData);
        SMgrRelationHash = hash_create("smgr relation table", 400,
                                     &ctl, HASH_ELEM | HASH_BLOBS);
        dlist_init(&unpinned_relns);
    }

    // Look up or create relation entry
    brlocator.locator = rlocator;
    brlocator.backend = backend;
    reln = (SMgrRelation) hash_search(SMgrRelationHash, &brlocator,
                                    HASH_ENTER, &found);

    // Initialize new relation objects
    if (!found) {
        // Initialize target block and cached block counts
        reln->smgr_targblock = InvalidBlockNumber;
        for (int i = 0; i <= MAX_FORKNUM; ++i)
            reln->smgr_cached_nblocks[i] = InvalidBlockNumber;

        // Set storage manager type (currently only md.c)
        reln->smgr_which = 0;

        // Add to unpinned list with zero pin count
        reln->pincount = 0;
        dlist_push_tail(&unpinned_relns, &reln->node);

        // Initialize storage manager implementation
        smgrsw[reln->smgr_which].smgr_open(reln);
    }

    return reln;
}
```

Key simplifications made:
- Added clear comments for hash table initialization and relation lookup phases
- Simplified the new relation initialization logic with descriptive comments
- Emphasized the caching and lifetime management aspects
- Preserved the essential lazy initialization pattern and storage manager abstraction