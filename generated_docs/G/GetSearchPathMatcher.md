# GetSearchPathMatcher

## Location
[src/backend/catalog/namespace.c:3852-3888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3852-L3888)

## Overview
Fetches the current search path definition and creates a SearchPathMatcher structure that encapsulates the namespace search order and characteristics.

## Definition

```c
SearchPathMatcher *
GetSearchPathMatcher(MemoryContext context)
```
## Detailed Description
This function creates a SearchPathMatcher structure that represents the current search path configuration. It recomputes the namespace path to ensure accuracy, then constructs a matcher object that includes the list of schemas to search, flags indicating whether temporary and catalog namespaces should be implicitly added, and a generation number for cache validation. The function allocates the result in the specified memory context while performing intermediate calculations in the current memory context.

## Parameters / Member Variables
- : The memory context in which to allocate the result structure

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathMatcher](../S/SearchPathMatcher.md) (type)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_copy](../l/list_copy.md)
  - linitial_oid
  - list_delete_first
  - [palloc0](../p/palloc0.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - activeSearchPath (global variable)
  - activeCreationNamespace (global variable)
  - myTempNamespace (global variable)
  - activePathGeneration (global variable)
  - PG_CATALOG_NAMESPACE (constant)
- Called from (representative examples):
  - [CompleteCachedPlan](../C/CompleteCachedPlan.md) (src/backend/utils/cache/plancache.c:436)
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md) (src/backend/utils/cache/plancache.c:787)
  - RangeVarGetRelid (src/include/catalog/namespace.h:168)

## Notes and Other Information
- Used primarily for plan caching and invalidation in PostgreSQL's query planning system
- The SearchPathMatcher allows comparison of search paths across different points in time
- Handles implicit addition of temporary and catalog namespaces to the search path
- Memory management is carefully handled with separate contexts for result and intermediate calculations
- Part of PostgreSQL's namespace resolution and query plan caching infrastructure