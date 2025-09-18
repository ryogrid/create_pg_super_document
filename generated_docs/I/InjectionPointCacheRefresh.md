# InjectionPointCacheRefresh

## Location
[src/backend/utils/misc/injection_point.c:420-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L420-L525)

## Overview
A static function that serves as the core workhorse for injection point lookups, managing the local cache and ensuring thread-safe access to shared memory injection point data.

## Definition


## Detailed Description
This function implements the core logic for finding and caching injection points. It handles the complex synchronization between shared memory and local cache, dealing with concurrent modifications that can occur during lookup operations. The function performs several key operations:

1. **Shared Memory Check**: Reads the current number of active injection points to determine if any exist
2. **Cache Validation**: Checks if a local cache entry exists and validates it against the shared memory version using generation counters
3. **Shared Memory Search**: Performs a lock-free search through the shared memory array using memory barriers to ensure data consistency
4. **Concurrent Modification Handling**: Uses generation counters and memory barriers to detect and handle concurrent modifications during the search
5. **Cache Management**: Updates the local cache with fresh data when valid entries are found

The function is designed to be lock-free during reads, using atomic operations and memory barriers to ensure data consistency even when injection points are being concurrently added or removed.

## Parameters / Member Variables
- : The unique identifier of the injection point to search for

## Return Value
- Returns a pointer to  if the injection point is found and successfully cached
- Returns  if the injection point is not found or if no injection points exist

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - pg_read_barrier
  - [hash_destroy](../h/hash_destroy.md)
  - [injection_point_cache_get](../i/injection_point_cache_get.md)
  - [injection_point_cache_remove](../i/injection_point_cache_remove.md)
  - [injection_point_cache_load](../i/injection_point_cache_load.md)
- Types referenced:
  - [InjectionPointEntry](InjectionPointEntry.md)
  - [InjectionPointCacheEntry](InjectionPointCacheEntry.md)
- Called from:
  - [InjectionPointRun](InjectionPointRun.md) (src/backend/utils/misc/injection_point.c:531)

## Notes and Other Information
- This is a static (internal) function not exposed outside the injection_point.c file
- Implements sophisticated lock-free synchronization using generation counters and memory barriers
- Handles race conditions gracefully - may return old, new, or NULL values during concurrent modifications
- Uses a two-phase read approach: first read generation, then data, then verify generation unchanged
- Automatically destroys the local cache when no injection points exist in shared memory
- The function is tolerant of concurrent attach/detach operations and provides eventual consistency
- Uses memcmp for name comparison with explicit length checking for efficiency
- Memory barriers ensure proper ordering between generation reads and data access