# RemovePgTempFiles

## Location
[src/backend/storage/file/fd.c:3271-3329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3271-L3329)

## Overview
Removes temporary files and temporary relation files left over from a previous postmaster session during server startup, providing cleanup of orphaned temporary files.

## Definition
```c
void RemovePgTempFiles(void)
```

## Detailed Description
This function performs comprehensive cleanup of temporary files that may have been left behind from a previous PostgreSQL server session. It's called during postmaster startup to ensure a clean slate. The function operates in several phases:

1. **Default tablespace cleanup**: Processes temporary files in the default tablespace ($PGDATA/base) by cleaning both general temporary files and temporary relation files
2. **Non-default tablespace cleanup**: Iterates through all tablespaces in pg_tblspc directory and cleans temporary files in each
3. **Graceful error handling**: Uses LOG-level error reporting for syscall failures and continues operation rather than failing startup

The function is designed to be robust - it will continue cleanup operations even if some files cannot be removed, ensuring that PostgreSQL can start successfully. This is particularly important during crash recovery scenarios where the remove_temp_files_after_crash GUC setting controls whether this cleanup occurs.

## Parameters / Member Variables
This function takes no parameters and operates on the current PostgreSQL data directory structure.

## Dependencies
- Functions called/Symbols referenced:
  - [RemovePgTempFilesInDir](RemovePgTempFilesInDir.md) (removes temporary files from specific directories)
  - [RemovePgTempRelationFiles](RemovePgTempRelationFiles.md) (removes temporary relation files)
  - [AllocateDir](../A/AllocateDir.md), ReadDirExtended, FreeDir (directory traversal functions)
  - PG_TEMP_FILES_DIR, TABLESPACE_VERSION_DIRECTORY (path constants)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (during normal server startup)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) (during crash recovery)

## Notes and Other Information
- This function is called only during postmaster startup, not during normal operation
- The remove_temp_files_after_crash GUC controls whether this function runs after crashes
- In EXEC_BACKEND builds, the top-level pgsql_tmp directory is handled separately to avoid race conditions
- The function reports errors at LOG level and continues processing rather than failing, prioritizing database availability
- Helps prevent accumulation of temporary files that could consume disk space after crashes
- Part of PostgreSQL's crash recovery and cleanup infrastructure

## Simplified Source

```c
// Simplified version of RemovePgTempFiles
void RemovePgTempFiles(void) {
    char temp_path[MAXPGPATH + 10 + sizeof(TABLESPACE_VERSION_DIRECTORY) + sizeof(PG_TEMP_FILES_DIR)];
    DIR *spc_dir;
    struct dirent *spc_de;

    // Step 1: Clean temporary files in default tablespace (base directory)
    snprintf(temp_path, sizeof(temp_path), "base/%s", PG_TEMP_FILES_DIR);
    RemovePgTempFilesInDir(temp_path, true, false);
    RemovePgTempRelationFiles("base");

    // Step 2: Process all non-default tablespaces
    spc_dir = AllocateDir("pg_tblspc");

    while ((spc_de = ReadDirExtended(spc_dir, "pg_tblspc", LOG)) != NULL) {
        // Skip current and parent directory entries
        if (strcmp(spc_de->d_name, ".") == 0 || strcmp(spc_de->d_name, "..") == 0)
            continue;

        // Clean temporary files in this tablespace
        snprintf(temp_path, sizeof(temp_path), "pg_tblspc/%s/%s/%s",
                 spc_de->d_name, TABLESPACE_VERSION_DIRECTORY, PG_TEMP_FILES_DIR);
        RemovePgTempFilesInDir(temp_path, true, false);

        // Clean temporary relation files in this tablespace
        snprintf(temp_path, sizeof(temp_path), "pg_tblspc/%s/%s",
                 spc_de->d_name, TABLESPACE_VERSION_DIRECTORY);
        RemovePgTempRelationFiles(temp_path);
    }

    FreeDir(spc_dir);

    // Note: EXEC_BACKEND pgsql_tmp directory handled separately to avoid race conditions
}
```

Key simplifications made:
- Removed detailed comments about crash recovery behavior and GUC settings
- Consolidated the two-phase cleanup approach into clear steps
- Simplified the tablespace iteration logic explanation
- Focused on the main execution path without edge case details
- Kept essential error handling references but removed implementation details