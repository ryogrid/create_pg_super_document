# RelationMapInitializePhase3

## Location
src/backend/utils/cache/relmapper.c: 692 - 712

## Overview
RelationMapInitializePhase3 completes the relation mapper initialization by loading the database-specific local relation map file after MyDatabaseId and DatabasePath have been established.

## Definition
```c
void RelationMapInitializePhase3(void)
```

## Detailed Description
This function represents the final phase of relation mapper initialization during PostgreSQL startup. It is called after MyDatabaseId has been determined and DatabasePath has been set up, enabling access to the database-specific local relation map file. Like Phase2, it handles bootstrap mode by doing nothing since map files don't exist during bootstrap. In normal operation, it loads the local relation map file that contains mappings specific to the current database, complementing the shared mappings loaded in Phase2. This enables complete relation mapping functionality for both shared and database-specific system catalogs.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - load_relmap_file
- Called from (representative examples):
  - RelationCacheInitializePhase3 (at src/backend/utils/cache/relcache.c:4112)

## Notes and Other Information
- Loads the local (database-specific) map file, not the shared map
- Requires MyDatabaseId and DatabasePath to be set before calling
- Skips loading during bootstrap mode when local map files haven't been created yet
- Uses load_relmap_file with parameters (false, false) indicating shared=false, fatal_on_error=false  
- Completes the multi-phase initialization sequence started by RelationMapInitialize and RelationMapInitializePhase2
- Essential for accessing database-specific system catalogs and user relations that have OID-to-filenode mappings