# ReleaseSysCache

## Location
[src/backend/utils/cache/syscache.c:269-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L269-L286)

## Overview
ReleaseSysCache releases a previously acquired reference count on a system cache tuple, allowing it to be freed when no longer in use.

## Definition
void ReleaseSysCache(HeapTuple tuple)

## Detailed Description
ReleaseSysCache is a simple wrapper function around ReleaseCatCache that decrements the reference count on a tuple obtained from a system cache search. When a tuple is retrieved from the system cache using functions like SearchSysCache1-4, it gains a reference count that prevents it from being freed. This function must be called to release that reference when the tuple is no longer needed, enabling proper memory management and cache cleanup.

## Parameters / Member Variables
- tuple: HeapTuple pointer to the cached tuple to be released

## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseCatCache](ReleaseCatCache.md)
- Called from (representative examples):
  - (Note: No direct references found in current analysis, but this is a fundamental cleanup function)

## Notes and Other Information
- Essential for proper memory management of system cache tuples
- Must be called for every tuple obtained via SearchSysCache functions to prevent memory leaks
- The function is a thin wrapper that delegates to the lower-level ReleaseCatCache function
- Failure to call this function for cached tuples can lead to memory leaks and cache bloat
- Should be called even if the tuple pointer is NULL (the underlying ReleaseCatCache handles NULL gracefully)

## Simplified Source

```c
void ReleaseSysCache(HeapTuple tuple) {
    // Simple wrapper that delegates to ReleaseCatCache
    // for system cache tuple cleanup
    ReleaseCatCache(tuple);
}
```