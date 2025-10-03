# recomputeNamespacePath

## Location
[src/backend/catalog/namespace.c:4299-4361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4299-L4361)

## Overview
Recomputes namespace search path derived variables when the path is invalid or the current user has changed, updating both base and active search path state.

## Definition
```c
static void
recomputeNamespacePath(void)
```

## Detailed Description
The recomputeNamespacePath function manages the consistency of PostgreSQL's namespace search path state variables. It performs validation checks to determine if recomputation is necessary, comparing the current user ID with the cached user and checking if the base search path is still valid.

When recomputation is needed, the function:
1. Retrieves cached search path information via cachedNamespacePath
2. Compares the cached data with current base state to detect changes
3. If changes are detected, updates the base search path variables in TopMemoryContext
4. Activates the updated search path by copying base variables to active variables
5. Increments the path generation counter to signal changes to other subsystems

The function ensures thread-safety by using appropriate memory contexts and maintains consistency between base and active search path state.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathCacheEntry](../S/SearchPathCacheEntry.md) (struct type)
  - [cachedNamespacePath](../c/cachedNamespacePath.md)
  - [equal](../e/equal.md)
  - [list_copy](../l/list_copy.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [RangeVarGetCreationNamespace](../R/RangeVarGetCreationNamespace.md)
  - [RelnameGetRelid](../R/RelnameGetRelid.md)
  - [RelationIsVisibleExt](../R/RelationIsVisibleExt.md)
  - [TypenameGetTypidExtended](../T/TypenameGetTypidExtended.md)
  - [FuncnameGetCandidates](../F/FuncnameGetCandidates.md)
  - [fetch_search_path](../f/fetch_search_path.md)

## Notes and Other Information
- This is a static function only accessible within namespace.c
- Uses TopMemoryContext to ensure search path data persists across memory context resets
- The activePathGeneration counter helps other subsystems detect when cached namespace-related data needs refreshing
- Performance optimization: avoids unnecessary recomputation when path and user haven't changed
- Critical for maintaining consistency between different namespace-related operations throughout a session

## Simplified Source

```c
static void
recomputeNamespacePath(void)
{
    Oid roleid = GetUserId();

    // Skip if path is already valid for current user
    if (baseSearchPathValid && namespaceUser == roleid)
        return;

    // Get cached search path data for current user
    const SearchPathCacheEntry *entry = cachedNamespacePath(namespace_search_path, roleid);

    // Check if path actually changed
    bool pathChanged = !(baseCreationNamespace == entry->firstNS &&
                         baseTempCreationPending == entry->temp_missing &&
                         equal(entry->finalPath, baseSearchPath));

    if (pathChanged)
    {
        // Update base search path in permanent memory context
        MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);
        List *newpath = list_copy(entry->finalPath);
        MemoryContextSwitchTo(oldcxt);

        // Replace old path with new one
        list_free(baseSearchPath);
        baseSearchPath = newpath;
        baseCreationNamespace = entry->firstNS;
        baseTempCreationPending = entry->temp_missing;
    }

    // Mark path as valid and activate it
    baseSearchPathValid = true;
    namespaceUser = roleid;
    activeSearchPath = baseSearchPath;
    activeCreationNamespace = baseCreationNamespace;
    activeTempCreationPending = baseTempCreationPending;

    // Increment generation counter if path changed
    if (pathChanged)
        activePathGeneration++;
}
```