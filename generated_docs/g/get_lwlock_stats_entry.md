# get_lwlock_stats_entry

## Location
[src/backend/storage/lmgr/lwlock.c:371-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L371-L407)

## Overview
Retrieves or creates a statistics entry for a specific lightweight lock, managing the hash table lookup and initialization of new entries.

## Definition

```c
static lwlock_stats *
get_lwlock_stats_entry(LWLock *lock)
```
## Detailed Description
This function serves as the primary interface for accessing lock statistics entries in the lwlock_stats hash table. It takes a lock pointer and returns the corresponding statistics structure, creating a new entry if one doesn't exist. The function handles the special case where the statistics hash table hasn't been initialized yet (during shared memory setup) by returning a dummy statistics entry.

When creating new entries, the function initializes all counters to zero. The hash key consists of the lock's tranche identifier and the lock instance pointer, ensuring each lock has a unique statistics entry.

## Parameters / Member Variables
- `*lock`: Pointer to the LWLock for which statistics are needed
## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - [hash_search](../h/hash_search.md)
- Types referenced:
  - [LWLock](../L/LWLock.md)
  - [lwlock_stats_key](../l/lwlock_stats_key.md)
  - [lwlock_stats](../l/lwlock_stats.md)
- Constants used:
  - HASH_ENTER
- Global variables accessed:
  - lwlock_stats_htab
  - lwlock_stats_dummy
- Called from:
  - [LWLockWaitListLock](../L/LWLockWaitListLock.md) (src/backend/storage/lmgr/lwlock.c:864)
  - [LWLockDequeueSelf](../L/LWLockDequeueSelf.md) (src/backend/storage/lmgr/lwlock.c:1088)
  - [LWLockAcquire](../L/LWLockAcquire.md) (src/backend/storage/lmgr/lwlock.c:1178)
  - [LWLockAcquireOrWait](../L/LWLockAcquireOrWait.md) (src/backend/storage/lmgr/lwlock.c:1406)
  - [LWLockWaitForVar](../L/LWLockWaitForVar.md) (src/backend/storage/lmgr/lwlock.c:1595)
  - LOG_LWDEBUG (src/backend/storage/lmgr/lwlock.c:309)

## Notes and Other Information
- Returns a pointer to the statistics entry, never NULL
- Creates hash table entries on demand using HASH_ENTER mode
- Uses the lock's tranche and instance pointer as the composite hash key
- Handles the bootstrap case when lwlock_stats_htab is NULL by returning lwlock_stats_dummy
- All new statistics entries are initialized with zero counters
- The function is thread-safe as hash_search handles concurrent access appropriately
- Only compiled and functional when LWLOCK_STATS debugging is enabled

## Simplified Source

```c
// Simplified version of get_lwlock_stats_entry
static lwlock_stats *
get_lwlock_stats_entry(LWLock *lock)
{
    lwlock_stats_key key;
    lwlock_stats *lwstats;
    bool found;

    // Handle bootstrap case - no hash table yet
    if (lwlock_stats_htab == NULL)
        return &lwlock_stats_dummy;

    // Create hash key from lock tranche and instance
    memset(&key, 0, sizeof(key));
    key.tranche = lock->tranche;
    key.instance = lock;

    // Find or create statistics entry in hash table
    lwstats = hash_search(lwlock_stats_htab, &key, HASH_ENTER, &found);

    // Initialize new entry with zero counters
    if (!found) {
        lwstats->sh_acquire_count = 0;
        lwstats->ex_acquire_count = 0;
        lwstats->block_count = 0;
        lwstats->dequeue_self_count = 0;
        lwstats->spin_delay_count = 0;
    }

    return lwstats;
}
```

Key simplifications made:
- Replaced MemSet macro with standard memset for clarity
- Added descriptive comments for each logical section
- Simplified variable declarations by grouping logically related ones
- Made the bootstrap check more prominent at the start
- Consolidated counter initialization into a clear block
- Preserved all essential logic and error handling