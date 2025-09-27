# init_lwlock_stats

## Location
[src/backend/storage/lmgr/lwlock.c:312-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L312-L346)

## Overview
Initializes the lightweight lock statistics tracking system, setting up a hash table to store lock statistics and registering a cleanup handler.

## Definition

```c
static void
init_lwlock_stats(void)
```
## Detailed Description
This function sets up the infrastructure for collecting lightweight lock statistics in PostgreSQL's debugging system. It creates a dedicated memory context for lock statistics, allocates a hash table to track individual lock usage patterns, and registers a cleanup function to print statistics at process exit. The function is designed to handle reinitialization by cleaning up any existing statistics context before creating a new one.

The function creates a hash table using the  as the key and  as the entry structure. The hash table is configured to allow allocations within critical sections, which is normally prohibited but acceptable for debugging code.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - AllocSetContextCreate  
  - [MemoryContextAllowInCriticalSection](../M/MemoryContextAllowInCriticalSection.md)
  - [hash_create](../h/hash_create.md)
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [print_lwlock_stats](../p/print_lwlock_stats.md)
- Types referenced:
  - [HASHCTL](../H/HASHCTL.md)
  - [lwlock_stats_key](../l/lwlock_stats_key.md)
  - [lwlock_stats](../l/lwlock_stats.md)
- Called from:
  - LOG_LWDEBUG (src/backend/storage/lmgr/lwlock.c:307)
  - [InitLWLockAccess](../I/InitLWLockAccess.md) (src/backend/storage/lmgr/lwlock.c:563)

## Notes and Other Information
- This function is only compiled when LWLOCK_STATS debugging is enabled
- Uses a static memory context that persists across function calls
- Allows memory allocation within critical sections, which is normally forbidden but acceptable for debugging code
- The hash table is sized for 16384 entries to accommodate tracking of many locks
- Registers print_lwlock_stats as an exit handler to output statistics when the process terminates
- Can be called multiple times safely as it cleans up existing context before reinitializing

## Simplified Source

```c
// Simplified version of init_lwlock_stats
static void init_lwlock_stats(void) {
    HASHCTL ctl;
    static MemoryContext lwlock_stats_cxt = NULL;
    static bool exit_registered = false;

    // Clean up existing statistics context if it exists
    if (lwlock_stats_cxt != NULL)
        MemoryContextDelete(lwlock_stats_cxt);

    // Create dedicated memory context for lock statistics
    // This context is allowed to allocate within critical sections for debugging
    lwlock_stats_cxt = AllocSetContextCreate(TopMemoryContext,
                                           "LWLock stats",
                                           ALLOCSET_DEFAULT_SIZES);
    MemoryContextAllowInCriticalSection(lwlock_stats_cxt, true);

    // Set up hash table configuration
    ctl.keysize = sizeof(lwlock_stats_key);
    ctl.entrysize = sizeof(lwlock_stats);
    ctl.hcxt = lwlock_stats_cxt;

    // Create hash table to store lock statistics (16384 buckets)
    lwlock_stats_htab = hash_create("lwlock stats", 16384, &ctl,
                                   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    // Register exit handler to print statistics (only once)
    if (!exit_registered) {
        on_shmem_exit(print_lwlock_stats, 0);
        exit_registered = true;
    }
}
```

Key simplifications made:
- Preserved the essential initialization flow
- Kept critical memory management and hash table setup
- Maintained the static variables and their purpose
- Simplified comments to focus on core functionality
- Removed the detailed technical comment about critical sections (summarized in inline comment)
- Maintained all essential function calls and logic flow