# InjectionPointDetach

## Location
src/backend/utils/misc/injection_point.c: 360 - 419

## Overview
Removes an existing injection point from the shared memory hash table by name, marking its slot as available for reuse and optionally compacting the active entry range.

## Definition


## Detailed Description
This function searches for and removes an injection point with the specified name from the shared memory array. It uses the same generation counter mechanism as InjectionPointAttach to ensure thread-safe operations. The function performs the following steps:

1. Acquires an exclusive lock on the injection point system
2. Searches through active entries to find a matching name
3. Marks the found entry as inactive by incrementing its generation counter (making it even)
4. If the removed entry was the highest-numbered active entry, updates the max_inuse counter to optimize future searches
5. Returns true if an entry was found and removed, false otherwise

The function optimizes the shared memory usage by compacting the active range when possible, reducing the search space for future operations.

## Parameters / Member Variables
- : The unique identifier of the injection point to remove

## Return Value
- Returns  if the injection point was successfully found and detached
- Returns  if no injection point with the given name was found

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md)
  - LWLockAcquire/LWLockRelease
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