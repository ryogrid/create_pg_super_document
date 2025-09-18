# InjectionPointCacheEntry

## Location
src/backend/utils/misc/injection_point.c: 94 - 107

## Overview
InjectionPointCacheEntry represents a backend-local cache entry for injection point callbacks that have been loaded, stored in TopMemoryContext for efficient access.

## Definition


## Detailed Description
InjectionPointCacheEntry is a backend-local cache structure that stores loaded injection point callbacks to avoid repeated lookups and loading operations. Each backend process maintains its own cache of injection points in TopMemoryContext, which persists for the lifetime of the backend process.

The cache entry includes validation fields (slot_idx and generation) that allow the backend to verify whether the cached entry is still valid against the shared memory version. This design allows for efficient callback execution while maintaining consistency with the shared injection point state.

## Parameters / Member Variables
- : Name of the cached injection point (maximum 64 characters including null terminator)
- : Copy of the private data associated with this injection point (maximum 1024 bytes)
- : Function pointer to the loaded callback function (type: void (*)(const char *name, const void *private_data))
- : Index of the corresponding slot in the shared memory InjectionPointsCtl entries array
- : Copy of the generation number from the shared memory entry when this cache entry was created, used for validation

## Dependencies
- Functions called/Symbols referenced:
  - INJ_NAME_MAXLEN (64)
  - INJ_PRIVATE_MAXLEN (1024)
  - InjectionPointCallback
- Called from (representative examples):
  - [injection_point_cache_add](../i/injection_point_cache_add.md)
  - [injection_point_cache_remove](../i/injection_point_cache_remove.md)
  - [injection_point_cache_load](../i/injection_point_cache_load.md)
  - [injection_point_cache_get](../i/injection_point_cache_get.md)
  - [InjectionPointDetach](InjectionPointDetach.md)
  - [InjectionPointCacheRefresh](InjectionPointCacheRefresh.md)
  - [InjectionPointRun](InjectionPointRun.md)

## Notes and Other Information
The cache is stored in TopMemoryContext to ensure it persists for the lifetime of the backend process. The slot_idx and generation fields provide a mechanism to validate cache entries against the shared memory state, allowing the system to detect when an injection point has been modified or removed by another process. This validation is crucial for maintaining consistency in a multi-process environment where injection points can be dynamically added or removed.