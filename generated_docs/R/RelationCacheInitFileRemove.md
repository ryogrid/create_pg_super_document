# RelationCacheInitFileRemove

## Location
[src/backend/utils/cache/relcache.c:6806-6838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6806-L6838)

## Overview
Removes all relation cache initialization files across all databases and tablespaces during postmaster startup to ensure cache consistency after restart.

## Definition
```c
void RelationCacheInitFileRemove(void)
```

## Detailed Description
This function systematically removes all relation cache initialization files from the PostgreSQL data directory during postmaster startup. It was designed to address safety concerns in PITR (Point-In-Time Recovery) scenarios and crash recovery situations where init files could become out-of-sync with the actual database state.

The function operates by:
1. Removing the global init file from the global directory
2. Scanning the default tablespace (base directory) for database-specific init files
3. Scanning all non-default tablespaces by examining the pg_tblspc directory
4. Removing init files from each database directory found

This approach ensures that the first backend to connect to each database will rebuild the relation cache initialization files from scratch, guaranteeing consistency with the current database state.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [unlink_initfile](../u/unlink_initfile.md)
  - [RelationCacheInitFileRemoveInDir](RelationCacheInitFileRemoveInDir.md)
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDirExtended](ReadDirExtended.md)
  - [FreeDir](../F/FreeDir.md)
  - RELCACHE_INIT_FILENAME
  - TABLESPACE_VERSION_DIRECTORY
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md)

## Notes and Other Information
- Previously, PostgreSQL kept init files across restarts, but this was deemed unsafe
- The function handles both default and non-default tablespaces systematically
- Uses LOG level error reporting, so failures to remove files are logged but don't cause fatal errors
- This operation occurs during postmaster startup before any backends are launched
- The first backend connecting to each database will rebuild the missing init files automatically