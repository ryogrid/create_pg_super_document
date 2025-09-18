# injection_point_cache_load

## Location
src/backend/utils/misc/injection_point.c: 176 - 208

## Overview
Loads an injection point callback from an external library and adds it to the local backend cache.

## Definition
```c
static InjectionPointCacheEntry *
injection_point_cache_load(InjectionPointEntry *entry, int slot_idx, uint64 generation)
```

## Detailed Description
This function loads injection point callbacks from external libraries into the local process cache. It constructs the full library path using the pkglib_path and DLSUFFIX, verifies the library file exists, then uses load_external_function to dynamically load the specified function from the library. Once the callback function is successfully loaded, it creates a cache entry using injection_point_cache_add. The function handles error cases by raising ERROR messages if the library file or function cannot be found.

## Parameters / Member Variables
- `entry`: Pointer to InjectionPointEntry structure from shared memory containing injection point details
- `slot_idx`: Index of the slot in shared memory where this injection point is stored  
- `generation`: Generation number from the shared memory slot for cache validation

## Dependencies
- Functions called/Symbols referenced:
  - snprintf: Constructs the full library path
  - pg_file_exists: Checks if the library file exists on disk
  - load_external_function: Dynamically loads the callback function from the library
  - elog: Reports errors if library or function cannot be found
  - injection_point_cache_add: Adds the loaded callback to the local cache
- Called from (representative examples):
  - InjectionPointCacheRefresh: Loads new injection points during cache refresh operations

## Notes and Other Information
- The function is static (internal to injection_point.c)
- Library path is constructed as: pkglib_path/library_name + DLSUFFIX
- Uses load_external_function with false flag for strict error handling
- Raises ERROR level messages (which abort the transaction) if loading fails
- The loaded callback function pointer is cast to void* for storage
- Private data from the shared memory entry is passed through to the cache entry