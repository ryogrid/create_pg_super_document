# load_relmap_file

## Location
[src/backend/utils/cache/relmapper.c:765-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L765-L783)

## Overview
load_relmap_file loads either the shared or local relation mapping file into memory, providing essential access to core system catalog mappings.

## Definition

```c
static void
load_relmap_file(bool shared, bool lock_held)
```
## Detailed Description
load_relmap_file is an internal function that loads relation mapping files from disk into memory. It handles both shared mappings (stored in the global directory) and local mappings (stored in the database-specific directory). These files are critical for PostgreSQL operation as they contain mappings between relation OIDs and their physical file locations for core system catalogs.

The function treats failure to load these files as a fatal error since they are essential for database operation. The shared map contains mappings for cluster-wide relations, while the local map contains mappings specific to a particular database.

## Parameters / Member Variables
- : Boolean flag indicating whether to load the shared mapping file (true) or local mapping file (false)
- : Boolean indicating whether the caller already holds the necessary lock for file operations

## Dependencies
- Functions called/Symbols referenced:
  - [read_relmap_file](../r/read_relmap_file.md) (underlying file reading function)
  - shared_map (global shared mapping structure)
  - local_map (global local mapping structure) 
  - DatabasePath (global variable for database directory path)
- Called from (representative examples):
  - [RelationMapInvalidate](../R/RelationMapInvalidate.md) (at src/backend/utils/cache/relmapper.c:473, 478)
  - [RelationMapInvalidateAll](../R/RelationMapInvalidateAll.md) (at src/backend/utils/cache/relmapper.c:493, 495)
  - [RelationMapInitializePhase2](../R/RelationMapInitializePhase2.md) (at src/backend/utils/cache/relmapper.c:682)
  - [RelationMapInitializePhase3](../R/RelationMapInitializePhase3.md) (at src/backend/utils/cache/relmapper.c:703)
  - [perform_relmap_update](../p/perform_relmap_update.md) (at src/backend/utils/cache/relmapper.c:1059)

## Notes and Other Information
- This is a static function, only accessible within the relmapper.c file
- Failure to load mapping files results in FATAL error level, terminating the process
- The local case requires DatabasePath to be properly initialized before calling
- Used during database startup, invalidation scenarios, and mapping updates
- Part of PostgreSQL's critical system catalog access infrastructure