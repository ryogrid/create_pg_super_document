# IndexScanOK

## Location
[src/backend/utils/cache/catcache.c:1247-1311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1247-L1311)

## Overview
IndexScanOK determines whether it's safe to use index scans for catalog cache lookups, preventing infinite recursion during relcache initialization and ensuring authentication works during startup.

## Definition
```c
static bool IndexScanOK(CatCache *cache, ScanKey cur_skey)
```

## Detailed Description
IndexScanOK is a static function that implements critical logic to prevent infinite recursion and bootstrap ordering problems during PostgreSQL initialization. The function determines whether index scans can be safely used for catalog cache lookups, or if heap scans must be used instead.

The function addresses several critical scenarios:

1. **Critical Relcache Bootstrap**: During initial system startup, some system indexes needed for catalog cache operations haven't been initialized yet. Using index scans on these would cause infinite recursion as the index initialization itself requires catalog lookups.

2. **Authentication Before Index Availability**: During backend startup, authentication must work even before all relcache entries for authentication-related indexes are available.

3. **Access Method Optimization**: For pg_am (access method) catalogs, heap scans are always preferred because the table is small and the optimization isn't worth the complexity.

The function uses global flags like criticalRelcachesBuilt and criticalSharedRelcachesBuilt to track the initialization state and make appropriate decisions. For most catalog caches and normal operation, index scans are preferred for performance.

## Parameters / Member Variables
- `cache`: Pointer to the CatCache structure being queried
- `cur_skey`: Scan key for the current lookup operation (currently unused but maintained for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - Uses global variables criticalRelcachesBuilt and criticalSharedRelcachesBuilt
  - References various cache ID constants (INDEXRELID, AMOID, AMNAME, etc.)
- Called from:
  - [SearchCatCacheMiss](../S/SearchCatCacheMiss.md) (to determine scan strategy during cache miss)
  - [SearchCatCacheList](../S/SearchCatCacheList.md) (to determine scan strategy for list searches)

## Notes and Other Information
- This is a static function, only accessible within catcache.c
- Critical for preventing bootstrap ordering problems during PostgreSQL startup
- The function always returns false for pg_am catalogs (AMOID, AMNAME) as an optimization
- Uses cache ID switch statement to handle different catalog-specific requirements
- Global initialization flags (criticalRelcachesBuilt, criticalSharedRelcachesBuilt) control behavior
- Essential for authentication system availability during startup
- The cur_skey parameter is currently unused but preserved for potential future enhancements
- Prevents infinite recursion that would occur if index scans were used before indexes are properly initialized
- Part of the careful bootstrap sequence that allows PostgreSQL to start up successfully

## Simplified Source

```c
static bool IndexScanOK(CatCache *cache, ScanKey cur_skey) {
    switch (cache->id) {
        case INDEXRELID:
            // Force heap scans for pg_index until critical relcaches built
            // (prevents infinite recursion during index initialization)
            if (!criticalRelcachesBuilt) {
                return false;
            }
            break;

        case AMOID:
        case AMNAME:
            // Always use heap scans for pg_am - table is small,
            // and required during critical relcache building
            return false;

        case AUTHNAME:
        case AUTHOID:
        case AUTHMEMMEMROLE:
        case DATABASEOID:
            // Protect authentication lookups before shared relcaches ready
            if (!criticalSharedRelcachesBuilt) {
                return false;
            }
            break;

        default:
            // Other caches can use index scans when available
            break;
    }

    // Normal case: index scans are safe to use
    return true;
}
```