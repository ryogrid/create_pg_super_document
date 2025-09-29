# sendTablespace

## Location
[src/backend/backup/basebackup.c:1134-1186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L1134-L1186)

## Overview
sendTablespace includes a tablespace directory in the output tar stream during base backup operations, handling auxiliary tablespaces (not PGDATA).

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This function processes auxiliary tablespace directories during PostgreSQL base backup operations. It constructs the path to the tablespace version directory, creates a directory entry in the tar stream with proper permissions, and recursively sends all files within that directory. The function handles cases where tablespaces may be removed during the backup process gracefully by returning 0 if the directory no longer exists.

## Parameters / Member Variables
- `sink`: bbsink object representing the backup destination stream
- `path`: File system path pointing to the tablespace location
- `spcoid`: Object identifier (OID) of the tablespace being processed
- `sizeonly`: Boolean flag - if true, only calculates total size without sending data
- `manifest`: Pointer to backup manifest information structure for tracking backup contents
- `ib`: Pointer to incremental backup information structure

## Dependencies
- Functions called/Symbols referenced:
  - lstat
  - [_tarWriteHeader](../t/_tarWriteHeader.md)
  - [sendDir](sendDir.md)
  - TABLESPACE_VERSION_DIRECTORY
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)

## Notes and Other Information
- Only used for auxiliary tablespaces, not for the main PGDATA directory
- Appends TABLESPACE_VERSION_DIRECTORY to the provided path to ensure only the correct version directory is included
- Gracefully handles tablespace removal during backup by checking for ENOENT errors
- Returns the total size of data processed/sent
- Part of PostgreSQL's base backup infrastructure located in src/backend/backup/basebackup.c:1134-1186

## Simplified Source

```c
// Simplified version of sendTablespace
static int64
sendTablespace(bbsink *sink, char *path, Oid spcoid, bool sizeonly,
               backup_manifest_info *manifest, IncrementalBackupInfo *ib)
{
    int64 size;
    char pathbuf[MAXPGPATH];
    struct stat statbuf;

    // Step 1: Build path to tablespace version directory
    snprintf(pathbuf, sizeof(pathbuf), "%s/%s", path, TABLESPACE_VERSION_DIRECTORY);

    // Step 2: Check if tablespace directory exists
    if (lstat(pathbuf, &statbuf) != 0) {
        if (errno != ENOENT) {
            // Report error for actual file system problems
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not stat file or directory \"%s\": %m", pathbuf)));
        }
        // Tablespace was removed during backup - not an error
        return 0;
    }

    // Step 3: Write directory header to tar stream with correct permissions
    size = _tarWriteHeader(sink, TABLESPACE_VERSION_DIRECTORY, NULL, &statbuf, sizeonly);

    // Step 4: Recursively send all files in the tablespace directory
    size += sendDir(sink, pathbuf, strlen(path), sizeonly, NIL, true, manifest, spcoid, ib);

    return size;
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Added step-by-step comments explaining the main logic flow
- Simplified variable declarations to focus on essential ones
- Maintained all core functionality while improving readability
- Preserved error handling for critical file system operations
- Kept the graceful handling of tablespace removal during backup