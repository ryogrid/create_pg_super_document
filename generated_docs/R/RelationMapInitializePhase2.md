# RelationMapInitializePhase2

## Location
[src/backend/utils/cache/relmapper.c:671-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L671-L691)

## Overview
RelationMapInitializePhase2 prepares the relation mapper for accessing pg_database during startup by loading the shared relation map file from disk.

## Definition
```c
void RelationMapInitializePhase2(void)
```

## Detailed Description
This function represents the second phase of relation mapper initialization during PostgreSQL startup. It is called when the system is ready to access pg_database and can read the shared relation map file from disk. The function handles two scenarios: in bootstrap mode, it does nothing since the map file doesn't exist yet; in normal mode, it loads the shared relation map file and fails fatally if the file cannot be read. This phase is critical for establishing the mapping between relation OIDs and their physical file nodes for shared system catalogs.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - [load_relmap_file](../l/load_relmap_file.md)
- Called from (representative examples):
  - [RelationCacheInitializePhase2](RelationCacheInitializePhase2.md) (at src/backend/utils/cache/relcache.c:4050)

## Notes and Other Information
- Only loads the shared map file, not the local database-specific map
- Skips loading during bootstrap mode when the shared map file hasn't been created yet
- Uses load_relmap_file with parameters (true, false) indicating shared=true, fatal_on_error=false
- Part of the multi-phase startup sequence that ensures proper ordering of system initialization
- Critical for accessing shared system catalogs like pg_database