# get_lwlock_stats_entry

## Location
src/backend/storage/lmgr/lwlock.c: 371 - 407

## Overview
Retrieves or creates a statistics entry for a specific lightweight lock, managing the hash table lookup and initialization of new entries.

## Definition


## Detailed Description
This function serves as the primary interface for accessing lock statistics entries in the lwlock_stats hash table. It takes a lock pointer and returns the corresponding statistics structure, creating a new entry if one doesn't exist. The function handles the special case where the statistics hash table hasn't been initialized yet (during shared memory setup) by returning a dummy statistics entry.

When creating new entries, the function initializes all counters to zero. The hash key consists of the lock's tranche identifier and the lock instance pointer, ensuring each lock has a unique statistics entry.

## Parameters / Member Variables
- : Pointer to the LWLock for which statistics are needed

## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - [hash_search](../h/hash_search.md)
- Types referenced:
  - [LWLock](../L/LWLock.md)
  - lwlock_stats_key
  - lwlock_stats
- Constants used:
  - HASH_ENTER
- Global variables accessed:
  - lwlock_stats_htab
  - lwlock_stats_dummy
- Called from:
  - LWLockWaitListLock (src/backend/storage/lmgr/lwlock.c:864)
  - LWLockDequeueSelf (src/backend/storage/lmgr/lwlock.c:1088)
  - LWLockAcquire (src/backend/storage/lmgr/lwlock.c:1178)
  - LWLockAcquireOrWait (src/backend/storage/lmgr/lwlock.c:1406)
  - LWLockWaitForVar (src/backend/storage/lmgr/lwlock.c:1595)
  - LOG_LWDEBUG (src/backend/storage/lmgr/lwlock.c:309)

## Notes and Other Information
- Returns a pointer to the statistics entry, never NULL
- Creates hash table entries on demand using HASH_ENTER mode
- Uses the lock's tranche and instance pointer as the composite hash key
- Handles the bootstrap case when lwlock_stats_htab is NULL by returning lwlock_stats_dummy
- All new statistics entries are initialized with zero counters
- The function is thread-safe as hash_search handles concurrent access appropriately
- Only compiled and functional when LWLOCK_STATS debugging is enabled