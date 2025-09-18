# init_lwlock_stats

## Location
src/backend/storage/lmgr/lwlock.c: 312 - 346

## Overview
Initializes the lightweight lock statistics tracking system, setting up a hash table to store lock statistics and registering a cleanup handler.

## Definition


## Detailed Description
This function sets up the infrastructure for collecting lightweight lock statistics in PostgreSQL's debugging system. It creates a dedicated memory context for lock statistics, allocates a hash table to track individual lock usage patterns, and registers a cleanup function to print statistics at process exit. The function is designed to handle reinitialization by cleaning up any existing statistics context before creating a new one.

The function creates a hash table using the  as the key and  as the entry structure. The hash table is configured to allow allocations within critical sections, which is normally prohibited but acceptable for debugging code.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - AllocSetContextCreate  
  - MemoryContextAllowInCriticalSection
  - [hash_create](../h/hash_create.md)
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [print_lwlock_stats](../p/print_lwlock_stats.md)
- Types referenced:
  - HASHCTL
  - lwlock_stats_key
  - lwlock_stats
- Called from:
  - LOG_LWDEBUG (src/backend/storage/lmgr/lwlock.c:307)
  - InitLWLockAccess (src/backend/storage/lmgr/lwlock.c:563)

## Notes and Other Information
- This function is only compiled when LWLOCK_STATS debugging is enabled
- Uses a static memory context that persists across function calls
- Allows memory allocation within critical sections, which is normally forbidden but acceptable for debugging code
- The hash table is sized for 16384 entries to accommodate tracking of many locks
- Registers print_lwlock_stats as an exit handler to output statistics when the process terminates
- Can be called multiple times safely as it cleans up existing context before reinitializing