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
- `context`: The memory context in which to allocate the result structure
## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathMatcher](../S/SearchPathMatcher.md) (type)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_copy](../l/list_copy.md)
  - linitial_oid
  - [list_delete_first](../l/list_delete_first.md)
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

## Simplified Source

```c
// Simplified version of GetSearchPathMatcher
SearchPathMatcher *GetSearchPathMatcher(MemoryContext context) {
    SearchPathMatcher *result;
    List *schemas;
    MemoryContext oldcxt;

    // Ensure search path is up to date
    recomputeNamespacePath();

    // Switch to target memory context for allocation
    oldcxt = MemoryContextSwitchTo(context);

    // Initialize the matcher structure
    result = (SearchPathMatcher *) palloc0(sizeof(SearchPathMatcher));
    schemas = list_copy(activeSearchPath);

    // Process schemas before the creation namespace
    while (schemas && linitial_oid(schemas) != activeCreationNamespace) {
        if (linitial_oid(schemas) == myTempNamespace) {
            result->addTemp = true;
        } else {
            // Must be catalog namespace
            Assert(linitial_oid(schemas) == PG_CATALOG_NAMESPACE);
            result->addCatalog = true;
        }
        schemas = list_delete_first(schemas);
    }

    // Set the final schema list and generation
    result->schemas = schemas;
    result->generation = activePathGeneration;

    // Restore original memory context
    MemoryContextSwitchTo(oldcxt);

    return result;
}
```

Key simplifications made:
- Simplified comments while preserving essential logic
- Consolidated the memory context management
- Preserved the essential search path processing logic
- Maintained the special handling for temp and catalog namespaces
- Focused on core workflow: update path, copy and process schemas, create matcher
- Kept critical memory context switching for proper allocation