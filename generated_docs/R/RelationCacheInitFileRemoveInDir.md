# RelationCacheInitFileRemoveInDir

## Location
[src/backend/utils/cache/relcache.c:6839-6862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6839-L6862)

## Overview
Helper function that removes relation cache initialization files from all database directories within a specific tablespace directory.

## Definition
```c
static void RelationCacheInitFileRemoveInDir(const char *tblspcpath)
```

## Detailed Description
This static helper function is used by RelationCacheInitFileRemove to process a single tablespace directory. It scans the provided tablespace path for database directories (identified by numeric directory names representing database OIDs) and removes the relation cache initialization file from each database directory found.

The function systematically:
1. Opens the specified tablespace directory
2. Iterates through all entries in the directory
3. Identifies database directories by checking if the directory name consists entirely of digits (database OIDs)
4. Constructs the full path to the init file within each database directory
5. Attempts to remove the init file using unlink_initfile

This approach ensures that relation cache initialization files are removed from all databases within the given tablespace, regardless of how many databases exist.

## Parameters / Member Variables
- `tblspcpath`: The path to the tablespace directory to process (e.g., "base" for default tablespace, or "pg_tblspc/12345/PG_17_202411111" for non-default tablespaces)

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDirExtended](ReadDirExtended.md)  
  - [FreeDir](../F/FreeDir.md)
  - [unlink_initfile](../u/unlink_initfile.md)
  - RELCACHE_INIT_FILENAME
- Called from (representative examples):
  - [RelationCacheInitFileRemove](RelationCacheInitFileRemove.md)

## Notes and Other Information
- This is a static function, only accessible within relcache.c
- Uses strspn to validate that directory names are purely numeric (representing database OIDs)
- Error handling is delegated to unlink_initfile, which logs errors appropriately
- The function handles both the default tablespace ("base") and non-default tablespaces uniformly
- Directory scanning uses ReadDirExtended with LOG level error handling for robustness