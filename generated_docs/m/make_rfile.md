# make_rfile

## Location
[src/bin/pg_combinebackup/reconstruct.c:510-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L510-L532)

## Overview
Allocates and performs basic initialization of an rfile structure for reading backup files, with optional handling for missing files.

## Definition

```c
static rfile *
make_rfile(char *filename, bool missing_ok)
```
## Detailed Description
This function creates and initializes a basic rfile structure for reading backup files. It allocates memory for the rfile, duplicates the filename string, and opens the file in read-only binary mode. The function provides flexible error handling through the missing_ok parameter: when set to true, it gracefully returns NULL if the file doesn't exist rather than terminating with a fatal error.

This function serves as the foundation for both full backup files and incremental backup files, providing the basic file handle and structure that other functions build upon to add format-specific metadata and functionality.

## Parameters / Member Variables
- `filename`: Path to the backup file to open and initialize
- `missing_ok`: If true, return NULL instead of fatal error when file doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md)
  - [pstrdup](../p/pstrdup.md)
  - open
  - [pg_free](../p/pg_free.md)
  - [pg_fatal](../p/pg_fatal.md)
  - PG_BINARY
  - O_RDONLY
- Called from (representative examples):
  - [make_incremental_rfile](make_incremental_rfile.md)
  - [reconstruct_from_incremental_file](../r/reconstruct_from_incremental_file.md)

## Notes and Other Information
The function opens files in binary mode (PG_BINARY) which is essential for reading PostgreSQL's block-oriented backup files correctly across different platforms. The missing_ok parameter allows callers to distinguish between optional files (like when checking if a full backup exists before looking for an incremental one) and required files that should cause fatal errors if missing.

## Simplified Source

```c
static rfile *make_rfile(char *filename, bool missing_ok)
{
    rfile *rf;

    // Allocate and initialize rfile structure
    rf = pg_malloc0(sizeof(rfile));
    rf->filename = pstrdup(filename);

    // Open file in read-only binary mode
    if ((rf->fd = open(filename, O_RDONLY | PG_BINARY, 0)) < 0) {
        // Handle missing file gracefully if allowed
        if (missing_ok && errno == ENOENT) {
            pg_free(rf);
            return NULL;
        }
        pg_fatal("could not open file \"%s\": %m", filename);
    }

    return rf;
}
```