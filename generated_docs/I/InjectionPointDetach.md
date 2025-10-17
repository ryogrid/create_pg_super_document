# InjectionPointDetach

## Location
[src/backend/utils/misc/injection_point.c:360-419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L360-L419)

## Overview
Removes an existing injection point from the shared memory hash table by name, marking its slot as available for reuse and optionally compacting the active entry range.

## Definition

```c
bool
InjectionPointDetach(const char *name)
```
## Detailed Description
This function searches for and removes an injection point with the specified name from the shared memory array. It uses the same generation counter mechanism as InjectionPointAttach to ensure thread-safe operations. The function performs the following steps:

1. Acquires an exclusive lock on the injection point system
2. Searches through active entries to find a matching name
3. Marks the found entry as inactive by incrementing its generation counter (making it even)
4. If the removed entry was the highest-numbered active entry, updates the max_inuse counter to optimize future searches
5. Returns true if an entry was found and removed, false otherwise

The function optimizes the shared memory usage by compacting the active range when possible, reducing the search space for future operations.

## Parameters / Member Variables
- `*name`: The unique identifier of the injection point to remove
## Return Value
- Returns  if the injection point was successfully found and detached
- Returns  if no injection point with the given name was found

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
- Types referenced:
  - [InjectionPointEntry](InjectionPointEntry.md)
  - [InjectionPointCacheEntry](InjectionPointCacheEntry.md)
- Called from:
  - [injection_points_cleanup](../i/injection_points_cleanup.md) (src/test/modules/injection_points/injection_points.c:172)
  - [injection_points_detach](../i/injection_points_detach.md) (src/test/modules/injection_points/injection_points.c:390)

## Notes and Other Information
- Only functional when compiled with USE_INJECTION_POINTS defined
- Uses exclusive locking to ensure thread-safe removal
- Employs generation counters where even numbers indicate inactive/free entries
- Searches from the highest index downward for efficiency
- Automatically compacts the max_inuse counter when removing the highest-numbered entry
- Does not require the injection point to exist - returns false if not found
- Used primarily for cleanup and testing scenarios in PostgreSQL development
- The function includes an assertion that ensures no duplicate names exist in the array

## Simplified Source

```c
bool
InjectionPointDetach(const char *name)
{
#ifdef USE_INJECTION_POINTS
    bool found = false;

    LWLockAcquire(InjectionPointLock, LW_EXCLUSIVE);

    // Search for entry with matching name (from highest index down)
    int max_inuse = pg_atomic_read_u32(&ActiveInjectionPoints->max_inuse);
    int idx;

    for (idx = max_inuse - 1; idx >= 0; --idx) {
        InjectionPointEntry *entry = &ActiveInjectionPoints->entries[idx];
        uint64 generation = pg_atomic_read_u64(&entry->generation);

        // Skip empty slots (even generation)
        if (generation % 2 == 0)
            continue;

        // Found matching entry - mark as inactive (make generation even)
        if (strcmp(entry->name, name) == 0) {
            found = true;
            pg_atomic_write_u64(&entry->generation, generation + 1);
            break;
        }
    }

    // Compact max_inuse if we removed the highest entry
    if (found && idx == max_inuse - 1) {
        // Find new highest active entry
        for (; idx >= 0; --idx) {
            InjectionPointEntry *entry = &ActiveInjectionPoints->entries[idx];
            uint64 generation = pg_atomic_read_u64(&entry->generation);
            if (generation % 2 != 0)  // Found active entry
                break;
        }
        pg_atomic_write_u32(&ActiveInjectionPoints->max_inuse, idx + 1);
    }

    LWLockRelease(InjectionPointLock);
    return found;
#else
    elog(ERROR, "Injection points are not supported by this build");
    return true;  // silence compiler
#endif
}
```