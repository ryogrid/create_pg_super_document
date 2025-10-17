# scan_directory

## Location
[src/bin/pg_checksums/pg_checksums.c:300-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_checksums/pg_checksums.c#L300-L432)

## Overview
The `scan_directory` function recursively traverses PostgreSQL data directories to discover and process data files for checksum operations, handling complex directory structures including tablespaces and relation segments.

## Definition
```c
static int64 scan_directory(const char *basedir, const char *subdir, bool sizeonly)
```

## Detailed Description
This function implements a sophisticated directory traversal algorithm specifically designed for PostgreSQL data directory structures. It serves dual purposes based on the `sizeonly` parameter:

1. **Size calculation mode**: When `sizeonly` is true, it calculates the total size of all data files without processing them, used for progress reporting initialization
2. **Processing mode**: When `sizeonly` is false, it processes each discovered data file through `scan_file` for checksum operations

Key capabilities include:

**File Discovery and Filtering:**
- Identifies PostgreSQL data files while filtering out system files, temporary files, and directories
- Handles file exclusions using the `skipfile` function for system files like `pg_control`
- Skips temporary files with `PG_TEMP_FILE_PREFIX` and temporary directories with `PG_TEMP_FILES_DIR`
- Filters out macOS-specific files like `.DS_Store`

**PostgreSQL File Structure Handling:**
- Parses relation file names to extract segment numbers (for files larger than 1GB split across multiple segments)
- Handles fork files (main, FSM, VM) by extracting the base filenode identifier
- Supports filenode filtering when `only_filenode` is specified for targeted processing

**Tablespace Support:**
- Provides special handling for `pg_tblspc` directory containing tablespace symbolic links
- Resolves tablespace links and processes their `TABLESPACE_VERSION_DIRECTORY` contents
- Validates tablespace structure before processing

**Recursive Processing:**
- Recursively processes subdirectories while maintaining proper path construction
- Self-calls for both regular subdirectories and tablespace locations
- Accumulates directory sizes across all recursive calls

## Parameters / Member Variables
- `basedir`: The base directory path (typically the PostgreSQL data directory)
- `subdir`: The subdirectory to process relative to basedir
- `sizeonly`: If true, only calculate total size; if false, process files for checksums

## Dependencies
- Functions called/Symbols referenced:
  - `[opendir](../o/opendir.md)`, `readdir`, `closedir` (POSIX directory traversal functions)
  - `lstat` (file system status function)
  - `snprintf`, `strcmp`, `strncmp`, `strlen` (standard C string functions)
  - `strchr`, `strlcpy`, `atoi` (string manipulation and parsing functions)
  - [skipfile](skipfile.md) (local function to check file exclusions)
  - [scan_file](scan_file.md) (local function to process individual data files)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
  - `S_ISREG`, `S_ISDIR`, `S_ISLNK` (POSIX file type macros)
  - `PG_TEMP_FILE_PREFIX`, `PG_TEMP_FILES_DIR`, `TABLESPACE_VERSION_DIRECTORY` (PostgreSQL constants)
  - Global variable: `only_filenode` (for filenode filtering)

- Called from (representative examples):
  - [main](../m/main.md) function in pg_checksums.c for initial directory processing
  - Self-recursive calls for subdirectories and tablespace processing

## Notes and Other Information
- This is a static function with internal linkage, accessible only within pg_checksums.c
- Returns the total size (in bytes) of all discovered data files
- Uses `MAXPGPATH` constant for path buffer sizing to handle PostgreSQLs maximum path length requirements
- Implements proper error handling for directory operations and file system calls
- The function handles PostgreSQLs file naming conventions:
  - Base files: `filenode` (e.g., "12345")  
  - Segment files: `filenode.N` (e.g., "12345.1", "12345.2")
  - Fork files: `filenode_fork` (e.g., "12345_fsm", "12345_vm")
- Tablespace processing follows PostgreSQLs tablespace directory structure where symbolic links in `pg_tblspc` point to actual tablespace locations
- The `sizeonly` mode is crucial for progress reporting as it allows calculating total work before beginning processing
- File filtering ensures that only actual relation data files are processed, avoiding system metadata files and temporary files
- Segment number parsing validates that segment numbers are greater than 0 (segment 0 is the base file without suffix)

## Simplified Source

```c
static int64
scan_directory(const char *basedir, const char *subdir, bool sizeonly)
{
    int64 dirsize = 0;
    char path[MAXPGPATH];
    DIR *dir;
    struct dirent *de;

    // Open directory for scanning
    snprintf(path, sizeof(path), "%s/%s", basedir, subdir);
    dir = opendir(path);
    if (!dir)
        pg_fatal("could not open directory \"%s\": %m", path);

    // Process each directory entry
    while ((de = readdir(dir)) != NULL)
    {
        char fn[MAXPGPATH];
        struct stat st;

        // Skip current/parent directory entries
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        // Skip temporary files and system files
        if (strncmp(de->d_name, PG_TEMP_FILE_PREFIX, strlen(PG_TEMP_FILE_PREFIX)) == 0 ||
            strncmp(de->d_name, PG_TEMP_FILES_DIR, strlen(PG_TEMP_FILES_DIR)) == 0 ||
            strcmp(de->d_name, ".DS_Store") == 0)
            continue;

        // Get file status
        snprintf(fn, sizeof(fn), "%s/%s", path, de->d_name);
        if (lstat(fn, &st) < 0)
            pg_fatal("could not stat file \"%s\": %m", fn);

        if (S_ISREG(st.st_mode))
        {
            // Process regular files (data files)
            char fnonly[MAXPGPATH];
            int segmentno = 0;

            if (skipfile(de->d_name))
                continue;

            // Parse filename to extract segment number and filenode
            strlcpy(fnonly, de->d_name, sizeof(fnonly));
            char *segmentpath = strchr(fnonly, '.');
            if (segmentpath != NULL)
            {
                *segmentpath++ = '\0';
                segmentno = atoi(segmentpath);
                if (segmentno == 0)
                    pg_fatal("invalid segment number in file \"%s\"", fn);
            }

            // Extract fork information
            char *forkpath = strchr(fnonly, '_');
            if (forkpath != NULL)
                *forkpath++ = '\0';

            // Filter by filenode if specified
            if (only_filenode && strcmp(only_filenode, fnonly) != 0)
                continue;

            dirsize += st.st_size;

            // Process file for checksums if not size-only mode
            if (!sizeonly)
                scan_file(fn, segmentno);
        }
        else if (S_ISDIR(st.st_mode) || S_ISLNK(st.st_mode))
        {
            // Handle subdirectories and tablespaces
            if (strncmp("pg_tblspc", subdir, strlen("pg_tblspc")) == 0)
            {
                // Special tablespace handling
                char tblspc_path[MAXPGPATH];
                snprintf(tblspc_path, sizeof(tblspc_path), "%s/%s",
                         path, de->d_name);
                dirsize += scan_directory(tblspc_path,
                                        TABLESPACE_VERSION_DIRECTORY,
                                        sizeonly);
            }
            else
            {
                // Regular subdirectory recursion
                dirsize += scan_directory(path, de->d_name, sizeonly);
            }
        }
    }

    closedir(dir);
    return dirsize;
}
```