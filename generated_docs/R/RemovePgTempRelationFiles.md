# RemovePgTempRelationFiles

## Location
[src/backend/storage/file/fd.c:3390-3417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3390-L3417)

## Overview
Processes one tablespace directory to find and clean up temporary relation files in per-database subdirectories.

## Definition
```c
static void RemovePgTempRelationFiles(const char *tsdirname)
```

## Detailed Description
This function traverses a tablespace directory looking for per-database subdirectories (identified by numeric names representing database OIDs). For each valid database directory found, it calls `RemovePgTempRelationFilesInDbspace` to clean up temporary relation files within that database space.

The function specifically filters directories to only process those with purely numeric names, which correspond to PostgreSQL database OIDs. This filtering automatically ignores non-database directories like "." and ".." as well as any other non-numeric directory names that might exist in the tablespace.

## Parameters / Member Variables
- `tsdirname`: Path to the tablespace directory to process

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDirExtended](ReadDirExtended.md)
  - [RemovePgTempRelationFilesInDbspace](RemovePgTempRelationFilesInDbspace.md)
  - [FreeDir](../F/FreeDir.md)
- Called from (representative examples):
  - [RemovePgTempFiles](RemovePgTempFiles.md)

## Notes and Other Information
- This is a static function, only accessible within the fd.c source file
- Uses `strspn()` to validate that directory names contain only digits (0-9)
- Automatically skips "." and ".." directories due to the numeric name validation
- Part of the PostgreSQL temporary file cleanup hierarchy: this function handles the tablespace level, delegating database-specific cleanup to `RemovePgTempRelationFilesInDbspace`
- Database directories are identified by their OID (Object Identifier) which is always numeric

## Simplified Source

```c
// Simplified version of RemovePgTempRelationFiles
static void RemovePgTempRelationFiles(const char *tsdirname) {
    DIR *ts_dir;
    struct dirent *de;
    char dbspace_path[MAXPGPATH * 2];

    // Open the tablespace directory
    ts_dir = AllocateDir(tsdirname);

    // Iterate through all entries in the tablespace directory
    while ((de = ReadDirExtended(ts_dir, tsdirname, LOG)) != NULL) {
        // Skip non-database directories (only process numeric names = database OIDs)
        if (strspn(de->d_name, "0123456789") != strlen(de->d_name))
            continue;

        // Build path to database directory
        snprintf(dbspace_path, sizeof(dbspace_path), "%s/%s",
                 tsdirname, de->d_name);

        // Recursively clean temp files in this database directory
        RemovePgTempRelationFilesInDbspace(dbspace_path);
    }

    // Clean up directory handle
    FreeDir(ts_dir);
}
```

Key simplifications made:
- Added descriptive comments for each major step
- Removed detailed comment block about numeric validation logic
- Consolidated the core algorithm into clear sequential steps
- Focused on the main execution path: open directory → filter database dirs → clean each → close