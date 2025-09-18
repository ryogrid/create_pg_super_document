# hash_destroy

## Location
src/backend/utils/hash/dynahash.c: 866 - 884

## Overview
Destroys a hash table by freeing all associated memory through destruction of its memory context.

## Definition
```c
void hash_destroy(HTAB *hashp)
```

## Detailed Description
The `hash_destroy` function provides a clean and efficient way to completely destroy a PostgreSQL hash table and free all associated memory. Rather than individually freeing each component (segments, directory, elements), it leverages PostgreSQL's memory context system to free everything at once by destroying the hash table's dedicated memory context. This approach ensures no memory leaks and handles all allocated structures automatically.

The function includes safety checks to verify that the hash table uses the standard dynamic allocation method and has its own memory context, ensuring that the destruction process is safe and complete.

## Parameters / Member Variables
- `hashp`: Pointer to the HTAB structure representing the hash table to be destroyed. Can be NULL (function handles this gracefully)

## Dependencies
- Functions called/Symbols referenced:
  - DynaHashAlloc (standard hash table allocation function - verified for safety)
  - hash_stats (collects and reports hash table statistics before destruction)
  - MemoryContextDelete (destroys the entire memory context, freeing all allocated memory)
- Called from (representative examples):
  - Various PostgreSQL subsystems including:
    - Hash index operations (_hash_finish_split)
    - Transaction processing (ReorderBufferReturnTXN, ReorderBufferTruncateTXN)
    - Catalog operations (find_all_inheritors)
    - Replication (tablesync_start_time_mapping, pgoutput_shutdown)
    - Storage management (ResetUnloggedRelationsInDbspaceDir)
    - Lock management (InitLocks, ReleasePredicateLocksLocal)

## Notes and Other Information
- Handles NULL input gracefully - no operation performed if hashp is NULL
- Requires that the hash table use DynaHashAlloc allocation method
- Requires that the hash table have its own dedicated memory context
- Uses PostgreSQL's memory context system for efficient bulk deallocation
- Calls hash_stats() to collect final statistics before destruction
- Widely used throughout PostgreSQL for cleanup in various subsystems
- More efficient than individual component deallocation due to context-based approach