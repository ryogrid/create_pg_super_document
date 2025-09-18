# assign_record_type_typmod

## Location
src/backend/utils/cache/typcache.c: 1953 - 2044

## Overview
Assigns a unique typmod value to a RECORD type TupleDesc by finding or creating a cache entry, enabling subsequent lookups via lookup_rowtype_tupdesc.

## Definition


## Detailed Description
This function is central to PostgreSQL's anonymous record type management system. It takes a TupleDesc for a RECORD type and ensures it has a unique typmod identifier that can be used for future lookups. The function manages a hash table (RecordCacheHash) that maps TupleDesc structures to typmod values, preventing duplicate cache entries for structurally identical record types.

The function first attempts to find an existing cache entry for the given TupleDesc. If found, it simply assigns the existing typmod to the input TupleDesc. If not found, it creates a new cache entry with a unique typmod, either by finding a matching shared tuple descriptor or by creating a local copy.

The process involves multiple steps: hash table initialization (if needed), cache lookup, shared registry consultation, tuple descriptor copying, cache array management, and hash table entry creation. All allocations are performed in CacheMemoryContext to ensure proper memory management.

## Parameters / Member Variables
- : TupleDesc for a RECORD type that needs a typmod assignment (must have tdtypeid == RECORDOID)

## Dependencies
- Functions called/Symbols referenced:
  - record_type_typmod_hash
  - record_type_typmod_compare  
  - hash_create
  - hash_search
  - CreateCacheMemoryContext
  - find_or_make_matching_shared_tupledesc
  - ensure_record_cache_typmod_slot_exists
  - CreateTupleDescCopy
- Called from (representative examples):
  - BlessTupleDesc (src/backend/executor/execTuples.c:2162)
  - SPI_returntuple (src/backend/executor/spi.c:1094)
  - ER_get_flat_size (src/backend/utils/adt/expandedrecord.c:672)
  - internal_get_result_type (src/backend/utils/fmgr/funcapi.c:469)

## Notes and Other Information
- Initializes RecordCacheHash hash table on first invocation with custom hash and comparison functions
- Uses HASH_ELEM, HASH_FUNCTION, and HASH_COMPARE flags for efficient hash table operation
- All memory allocations occur in CacheMemoryContext for proper cache memory management
- Assigns unique tuple descriptor identifiers via tupledesc_id_counter for additional tracking
- Integrates with shared record type registry system for cross-session record type sharing
- The assigned typmod value is stored in both the cache entry and the input TupleDesc's tdtypmod field
- Ensures cache array slots exist before creating new entries to prevent allocation failures