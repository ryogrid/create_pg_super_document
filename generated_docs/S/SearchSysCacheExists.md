# SearchSysCacheExists

## Location
[src/backend/utils/cache/syscache.c:426-448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L426-L448)

## Overview
SearchSysCacheExists is a convenience function that checks whether a tuple exists in the system cache without retaining any locks on the cache entry.

## Definition
```c
bool SearchSysCacheExists(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4)
```

## Detailed Description
This function provides a simple and efficient way to test for the existence of a tuple in the system cache. It searches for a tuple matching the provided keys using SearchSysCache, checks if a valid tuple was found, and immediately releases the tuple if found. The function returns a boolean value indicating whether the tuple exists, without providing access to the actual tuple data.

The function is designed as a lightweight probe operation that doesn't hold any locks on the cache entry after completion. This makes it suitable for existence checks where the caller only needs to know if a tuple is present but doesn't need to access its contents or ensure its continued availability.

## Parameters / Member Variables
- `cacheId`: The identifier of the system cache to search in (corresponds to entries in SysCacheIdentifier enum)
- `key1`: The first search key value
- `key2`: The second search key value (can be unused depending on cache structure)
- `key3`: The third search key value (can be unused depending on cache structure)
- `key4`: The fourth search key value (can be unused depending on cache structure)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache](SearchSysCache.md)
  - HeapTupleIsValid
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - SearchSysCacheExists1 (macro wrapper for single key searches)
  - SearchSysCacheExists2 (macro wrapper for two key searches)
  - SearchSysCacheExists3 (macro wrapper for three key searches)
  - SearchSysCacheExists4 (macro wrapper for four key searches)

## Notes and Other Information
- This function is typically accessed through convenience macros (SearchSysCacheExists1, SearchSysCacheExists2, etc.) that provide the appropriate number of keys for specific cache types
- Returns `true` if a matching tuple exists, `false` otherwise
- No locks are retained after the function completes, making it safe for use in situations where long-term cache consistency is not required
- The function is optimized for existence checking rather than data retrieval
- All four key parameters are provided for maximum flexibility, but not all may be used depending on the specific cache being searched

## Simplified Source

```c
bool SearchSysCacheExists(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4) {
    HeapTuple tuple;

    // Search for tuple in system cache
    tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
    if (!HeapTupleIsValid(tuple))
        return false;

    // Release immediately and return existence indicator
    ReleaseSysCache(tuple);
    return true;
}
```