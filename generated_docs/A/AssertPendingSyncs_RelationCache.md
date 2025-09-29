# AssertPendingSyncs_RelationCache

## Location
[src/backend/utils/cache/relcache.c:3166-3236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3166-L3236)

## Overview
Asserts that the relcache.c and storage.c modules agree on whether to skip WAL (Write-Ahead Logging) for relations, ensuring consistency between these two critical subsystems.

## Definition

```c
void
AssertPendingSyncs_RelationCache(void)
```
## Detailed Description
This function serves as a debugging and consistency check mechanism that verifies WAL-skipping decisions are synchronized between the relation cache (relcache.c) and storage management (storage.c). It opens every relation that the current transaction has locked and recreates relcache entries that might have been invalidated due to inconsistent WAL-skipping states.

The function works by iterating through all locks held by the current transaction, identifying relation locks, and attempting to open those relations. This process forces the recreation of relcache entries that may have been destroyed by CommandCounterIncrement() due to local invalidation messages when there's a mismatch between storage.c skipping WAL and relcache.c not skipping WAL.

Additionally, it iterates through all entries in the RelationIdCache hash table and calls AssertPendingSyncConsistency() on each relation descriptor to verify consistency.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [GetLockMethodLocalHash](../G/GetLockMethodLocalHash.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [RelationIdGetRelation](../R/RelationIdGetRelation.md)
  - RelationIsValid
  - [RelationClose](../R/RelationClose.md)
  - [AssertPendingSyncConsistency](AssertPendingSyncConsistency.md)
  - [repalloc](../r/repalloc.md)
- Data structures used:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [LOCALLOCK](../L/LOCALLOCK.md)
  - [RelIdCacheEnt](../R/RelIdCacheEnt.md)
  - LOCKTAG_RELATION
- Called from:
  - [smgrDoPendingSyncs](../s/smgrDoPendingSyncs.md) (in storage.c)

## Notes and Other Information
- This is primarily a debugging/assertion function used to catch inconsistencies between relcache and storage subsystems
- The function uses transaction snapshots to ensure consistent view during the check
- It dynamically allocates and reallocates memory for the relations array as needed
- Only processes relations that are actually locked by the current transaction
- The function is critical for maintaining data integrity in PostgreSQL's WAL-skipping optimizations

## Simplified Source

```c
void AssertPendingSyncs_RelationCache(void)
{
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock;
    Relation *rels;
    int maxrels;
    int nrels;
    RelIdCacheEnt *idhentry;
    int i;

    // Set up transaction snapshot for consistent view
    PushActiveSnapshot(GetTransactionSnapshot());

    // Initialize relations array
    maxrels = 1;
    rels = palloc(maxrels * sizeof(*rels));
    nrels = 0;

    // Iterate through all locks held by current transaction
    hash_seq_init(&status, GetLockMethodLocalHash());
    while ((locallock = (LOCALLOCK *) hash_seq_search(&status)) != NULL) {
        Oid relid;
        Relation r;

        // Skip locks that aren't held or aren't relation locks
        if (locallock->nLocks <= 0)
            continue;
        if ((LockTagType) locallock->tag.lock.locktag_type != LOCKTAG_RELATION)
            continue;

        // Get relation ID and open the relation
        relid = ObjectIdGetDatum(locallock->tag.lock.locktag_field2);
        r = RelationIdGetRelation(relid);
        if (!RelationIsValid(r))
            continue;

        // Expand array if needed
        if (nrels >= maxrels) {
            maxrels *= 2;
            rels = repalloc(rels, maxrels * sizeof(*rels));
        }
        rels[nrels++] = r;
    }

    // Check consistency for all cached relations
    hash_seq_init(&status, RelationIdCache);
    while ((idhentry = (RelIdCacheEnt *) hash_seq_search(&status)) != NULL)
        AssertPendingSyncConsistency(idhentry->reldesc);

    // Clean up: close all opened relations
    for (i = 0; i < nrels; i++)
        RelationClose(rels[i]);

    PopActiveSnapshot();
}
```