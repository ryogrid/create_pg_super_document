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