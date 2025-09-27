# InitializeSearchPath

## Location
[src/backend/catalog/namespace.c:4736-4795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4736-L4795)

## Overview
Initializes the search path module during PostgreSQL startup, setting up either bootstrap mode with a fixed pg_catalog path or normal mode with syscache invalidation callbacks.

## Definition

```c
void
InitializeSearchPath(void)
```
## Detailed Description
This function is called during InitPostgres to properly initialize the search path subsystem after the system is sufficiently initialized to perform catalog lookups. It handles two distinct initialization modes:

**Bootstrap Mode**: When the database is being bootstrapped, it sets up a fixed search path containing only 'pg_catalog' to ensure system tables are created in the correct namespace, ignoring any GUC settings.

**Normal Mode**: In regular operation, it registers syscache invalidation callbacks for various system catalogs that can affect search path resolution, including namespaces, roles, role memberships, and databases. It then marks the search path as invalid to force recomputation on first use.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - list_make1_oid
  - [GetUserId](../G/GetUserId.md)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)
  - [InvalidationCallback](InvalidationCallback.md)
- Called from (representative examples):
  - [InitPostgres](InitPostgres.md)
  - RangeVarGetRelid (via namespace.h)

## Notes and Other Information
- Must be called after the system is sufficiently initialized for catalog access
- In bootstrap mode, allocates the base search path in TopMemoryContext for persistence
- Sets up comprehensive invalidation callbacks to ensure search path cache consistency
- Uses lazy evaluation by marking paths invalid rather than immediately recomputing them
- Critical for proper namespace resolution throughout the PostgreSQL session
- The function increments activePathGeneration in bootstrap mode as a pro forma operation

## Simplified Source

```c
// Simplified version of InitializeSearchPath
void InitializeSearchPath(void) {
    if (IsBootstrapProcessingMode()) {
        // Bootstrap mode: Set fixed pg_catalog search path
        MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);
        baseSearchPath = list_make1_oid(PG_CATALOG_NAMESPACE);
        MemoryContextSwitchTo(oldcxt);

        // Initialize all base settings
        baseCreationNamespace = PG_CATALOG_NAMESPACE;
        baseTempCreationPending = false;
        baseSearchPathValid = true;
        namespaceUser = GetUserId();

        // Copy base settings to active settings
        activeSearchPath = baseSearchPath;
        activeCreationNamespace = baseCreationNamespace;
        activeTempCreationPending = baseTempCreationPending;
        activePathGeneration++;
    } else {
        // Normal mode: Register syscache invalidation callbacks
        CacheRegisterSyscacheCallback(NAMESPACEOID, InvalidationCallback, (Datum) 0);
        CacheRegisterSyscacheCallback(AUTHOID, InvalidationCallback, (Datum) 0);
        CacheRegisterSyscacheCallback(AUTHMEMROLEMEM, InvalidationCallback, (Datum) 0);
        CacheRegisterSyscacheCallback(DATABASEOID, InvalidationCallback, (Datum) 0);

        // Force search path recomputation on next use
        baseSearchPathValid = false;
        searchPathCacheValid = false;
    }
}
```

Key simplifications made:
- Removed detailed comments explaining each callback registration
- Consolidated variable assignments in bootstrap mode
- Focused on the two main execution paths
- Maintained the essential logic flow and all critical operations