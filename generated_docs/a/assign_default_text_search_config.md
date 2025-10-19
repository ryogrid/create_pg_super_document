# assign_default_text_search_config

## Location
[src/backend/utils/cache/ts_cache.c:670-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L670-L674)

## Overview
A GUC assign hook function that invalidates the cached text search configuration when the default_text_search_config parameter is changed.

## Definition
void assign_default_text_search_config(const char *newval, void *extra)

## Detailed Description
This function serves as an assignment hook for the default_text_search_config GUC parameter. Its primary purpose is to maintain cache consistency by invalidating the cached OID value (TSCurrentConfigCache) whenever the configuration setting changes. This ensures that subsequent calls to getTSCurrentConfig() will perform a fresh lookup of the new configuration rather than returning a stale cached value. The function is intentionally simple and lightweight, containing only the essential cache invalidation logic.

## Parameters / Member Variables
- `newval`: The new value being assigned to the default_text_search_config parameter (not used in the function)
- `extra`: Additional data associated with the assignment (not used in the function)

## Dependencies
- Functions called/Symbols referenced:
  - (None - only sets TSCurrentConfigCache global variable)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- Part of the GUC hook system that ensures cache consistency across parameter changes
- Works in conjunction with getTSCurrentConfig() which relies on the TSCurrentConfigCache
- The function doesn't validate the newval parameter as that's handled by check_default_text_search_config()
- Extremely lightweight operation that only performs cache invalidation
- Essential for maintaining correct behavior when text search configuration is changed at runtime

## Simplified Source

```c
void assign_default_text_search_config(const char *newval, void *extra) {
    // Invalidate cached configuration to force fresh lookup
    TSCurrentConfigCache = InvalidOid;
}
```