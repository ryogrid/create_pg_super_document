# injection_point_cache_remove

## Location
src/backend/utils/misc/injection_point.c: 162 - 175

## Overview
Removes an injection point entry from the local backend cache by name, using hash table removal operations.

## Definition
```c
static void
injection_point_cache_remove(const char *name)
```

## Detailed Description
This function removes a cached injection point entry from the local hash table cache. It performs a hash table removal operation and asserts that the entry was found and successfully removed. The function includes a comment noting that it may leak a dynamically loaded callback, which is considered acceptable for testing purposes. The function is primarily used during cache refresh operations when injection points are removed from shared memory.

## Parameters / Member Variables
- `name`: Name of the injection point to be removed from the cache (up to INJ_NAME_MAXLEN=64 characters)

## Dependencies
- Functions called/Symbols referenced:
  - hash_search: Performs hash table removal with HASH_REMOVE flag
  - Assert: Verifies the entry was found and removed successfully
  - PG_USED_FOR_ASSERTS_ONLY: Macro to mark the found variable as used only for assertions
- Called from (representative examples):
  - InjectionPointCacheRefresh: Refreshes the cache by removing outdated entries

## Notes and Other Information
- The function is static (internal to injection_point.c)
- Uses HASH_REMOVE flag to remove entries from the hash table
- Intentionally leaks dynamically loaded callbacks, which is acceptable for testing scenarios
- The found variable is marked with PG_USED_FOR_ASSERTS_ONLY to avoid compiler warnings in non-assert builds
- Assumes the InjectionPointCache hash table has already been initialized