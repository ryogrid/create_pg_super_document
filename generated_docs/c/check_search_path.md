# check_search_path

## Location
[src/backend/catalog/namespace.c:4657-4712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4657-L4712)

## Overview
check_search_path is a GUC (Grand Unified Configuration) validation hook that validates the syntactic correctness of new search_path values before they are accepted by PostgreSQL.

## Definition

```c
bool
check_search_path(char **newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for PostgreSQL's search_path configuration parameter. It implements a two-phase validation approach:

1. **Cache-based validation**: If a search path cache is available and the current user has been determined, the function first checks if the exact search_path string has been previously validated for the current user.

2. **Syntactic validation**: If cache lookup fails or cache is unavailable, the function performs syntactic validation by:
   - Creating a modifiable copy of the search path string
   - Using SplitIdentifierString to parse the comma-separated list of schema names
   - Checking that the syntax is valid (proper identifier format and comma separation)

The function intentionally does NOT validate whether the named schemas actually exist, as there are many valid use cases for search paths that include non-existent schemas, and the function may be called outside of transaction context where catalog access is not possible.

Upon successful validation, if caching is enabled, an empty cache entry is created for future lookups.

## Parameters / Member Variables
- : Pointer to the new search_path value to be validated
- : Pointer to extra data (unused in this implementation)
- : GucSource indicating where the new value originated from

## Dependencies
- Functions called/Symbols referenced:
  - GucSource
  - [spcache_init](../s/spcache_init.md)
  - [spcache_lookup](../s/spcache_lookup.md)
  - [SplitIdentifierString](../S/SplitIdentifierString.md)
  - GUC_check_errdetail
  - [list_free](../l/list_free.md)
  - [spcache_insert](../s/spcache_insert.md)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (Grand Unified Configuration) system
- Returns true if the search path is syntactically valid, false otherwise
- Uses caching mechanism to avoid repeatedly parsing the same search path strings
- The function prioritizes syntactic validation over semantic validation (existence of schemas)
- Error messages are set via GUC_check_errdetail when validation fails
- Memory management includes proper cleanup of temporary allocations (rawname, namelist)
- The cache is role-specific, allowing different users to have different cached search paths