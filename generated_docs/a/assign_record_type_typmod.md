# assign_record_type_typmod

## Location
[src/backend/utils/cache/typcache.c:1953-2044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1953-L2044)

## Overview
Assigns a unique typmod value to a RECORD type TupleDesc by finding or creating a cache entry, enabling subsequent lookups via lookup_rowtype_tupdesc.

## Definition

```c
void
assign_record_type_typmod(TupleDesc tupDesc)
```
## Detailed Description
This function is central to PostgreSQL's anonymous record type management system. It takes a TupleDesc for a RECORD type and ensures it has a unique typmod identifier that can be used for future lookups. The function manages a hash table (RecordCacheHash) that maps TupleDesc structures to typmod values, preventing duplicate cache entries for structurally identical record types.

The function first attempts to find an existing cache entry for the given TupleDesc. If found, it simply assigns the existing typmod to the input TupleDesc. If not found, it creates a new cache entry with a unique typmod, either by finding a matching shared tuple descriptor or by creating a local copy.

The process involves multiple steps: hash table initialization (if needed), cache lookup, shared registry consultation, tuple descriptor copying, cache array management, and hash table entry creation. All allocations are performed in CacheMemoryContext to ensure proper memory management.

## Parameters / Member Variables
- : TupleDesc for a RECORD type that needs a typmod assignment (must have tdtypeid == RECORDOID)

## Dependencies
- Functions called/Symbols referenced:
  - [record_type_typmod_hash](../r/record_type_typmod_hash.md)
  - [record_type_typmod_compare](../r/record_type_typmod_compare.md)  
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md)
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md)
  - [ensure_record_cache_typmod_slot_exists](../e/ensure_record_cache_typmod_slot_exists.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
- Called from (representative examples):
  - [BlessTupleDesc](../B/BlessTupleDesc.md) (src/backend/executor/execTuples.c:2162)
  - [SPI_returntuple](../S/SPI_returntuple.md) (src/backend/executor/spi.c:1094)
  - [ER_get_flat_size](../E/ER_get_flat_size.md) (src/backend/utils/adt/expandedrecord.c:672)
  - [internal_get_result_type](../i/internal_get_result_type.md) (src/backend/utils/fmgr/funcapi.c:469)

## Notes and Other Information
- Initializes RecordCacheHash hash table on first invocation with custom hash and comparison functions
- Uses HASH_ELEM, HASH_FUNCTION, and HASH_COMPARE flags for efficient hash table operation
- All memory allocations occur in CacheMemoryContext for proper cache memory management
- Assigns unique tuple descriptor identifiers via tupledesc_id_counter for additional tracking
- Integrates with shared record type registry system for cross-session record type sharing
- The assigned typmod value is stored in both the cache entry and the input TupleDesc's tdtypmod field
- Ensures cache array slots exist before creating new entries to prevent allocation failures

## Simplified Source

```c
void assign_record_type_typmod(TupleDesc tupDesc) {
    RecordCacheEntry *recentry;
    TupleDesc entDesc;
    bool found;
    MemoryContext oldcxt;

    Assert(tupDesc->tdtypeid == RECORDOID);

    // Initialize hash table on first use
    if (RecordCacheHash == NULL) {
        HASHCTL ctl;
        ctl.keysize = sizeof(TupleDesc);
        ctl.entrysize = sizeof(RecordCacheEntry);
        ctl.hash = record_type_typmod_hash;
        ctl.match = record_type_typmod_compare;
        RecordCacheHash = hash_create("Record information cache", 64, &ctl,
                                    HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

        if (!CacheMemoryContext)
            CreateCacheMemoryContext();
    }

    // Look for existing cache entry
    recentry = hash_search(RecordCacheHash, &tupDesc, HASH_FIND, &found);
    if (found && recentry->tupdesc != NULL) {
        tupDesc->tdtypmod = recentry->tupdesc->tdtypmod;
        return;
    }

    // Create new entry in cache memory context
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // Try to find or create matching tuple descriptor
    entDesc = find_or_make_matching_shared_tupledesc(tupDesc);
    if (entDesc == NULL) {
        // Create local copy with new typmod
        ensure_record_cache_typmod_slot_exists(NextRecordTypmod);
        entDesc = CreateTupleDescCopy(tupDesc);
        entDesc->tdrefcount = 1;
        entDesc->tdtypmod = NextRecordTypmod++;
    } else {
        ensure_record_cache_typmod_slot_exists(entDesc->tdtypmod);
    }

    // Store in cache array and assign unique ID
    RecordCacheArray[entDesc->tdtypmod].tupdesc = entDesc;
    RecordCacheArray[entDesc->tdtypmod].id = ++tupledesc_id_counter;

    // Create hash table entry
    recentry = hash_search(RecordCacheHash, &tupDesc, HASH_ENTER, NULL);
    recentry->tupdesc = entDesc;

    // Update caller's tuple descriptor
    tupDesc->tdtypmod = entDesc->tdtypmod;

    MemoryContextSwitchTo(oldcxt);
}
```