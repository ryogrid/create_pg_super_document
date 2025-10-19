# identify_target_directory

## Location
[src/bin/pg_waldump/pg_waldump.c:292-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L292-L337)

## Overview
Identifies and returns the valid target directory containing WAL files by searching through a predefined hierarchy of potential locations.

## Definition
```c
static char *identify_target_directory(char *directory, char *fname)
```

## Detailed Description
This function implements a systematic search strategy to locate WAL files across multiple potential directory locations. It follows a specific search hierarchy based on whether an explicit directory is provided or not.

When a directory is specified, it searches in:
1. The specified directory itself
2. The XLOGDIR subdirectory within the specified directory

When no directory is specified (directory is NULL), it searches in:
1. Current working directory (".")
2. XLOGDIR (typically "pg_wal") 
3. $PGDATA/XLOGDIR if the PGDATA environment variable is set

The function calls search_directory() for each potential location and returns a dynamically allocated copy of the first successful directory path. If no valid directory is found, it terminates the program with a fatal error message.

## Parameters / Member Variables
- `directory`: A string specifying the base directory to search, or NULL to use the default search hierarchy
- `fname`: The specific WAL filename to search for, or NULL to search for any valid WAL file

## Dependencies
- Functions called/Symbols referenced:
  - [search_directory](../s/search_directory.md)
  - [pg_strdup](../p/pg_strdup.md)
  - snprintf
  - getenv
  - [pg_fatal](../p/pg_fatal.md)
  - XLOGDIR (constant)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Returns a dynamically allocated string containing the valid target directory path (caller must free)
- The XLOGDIR constant typically refers to "pg_wal" directory in PostgreSQL data directories
- Uses MAXPGPATH to ensure constructed paths don't exceed system limits
- Terminates program execution if no valid WAL files can be located in any of the search locations
- Part of pg_waldump's initialization process to establish the working directory for WAL file analysis
- The function provides flexible WAL file location discovery suitable for various PostgreSQL installation configurations

## Simplified Source

```c
static char *identify_target_directory(char *directory, char *fname) {
    char fpath[MAXPGPATH];

    if (directory != NULL) {
        // Search in specified directory first
        if (search_directory(directory, fname))
            return pg_strdup(directory);

        // Then try directory/XLOGDIR
        snprintf(fpath, MAXPGPATH, "%s/%s", directory, XLOGDIR);
        if (search_directory(fpath, fname))
            return pg_strdup(fpath);
    } else {
        // Search in current directory
        if (search_directory(".", fname))
            return pg_strdup(".");

        // Search in XLOGDIR
        if (search_directory(XLOGDIR, fname))
            return pg_strdup(XLOGDIR);

        // Search in $PGDATA/XLOGDIR if PGDATA is set
        const char *datadir = getenv("PGDATA");
        if (datadir != NULL) {
            snprintf(fpath, MAXPGPATH, "%s/%s", datadir, XLOGDIR);
            if (search_directory(fpath, fname))
                return pg_strdup(fpath);
        }
    }

    // Could not locate WAL file anywhere
    if (fname)
        pg_fatal("could not locate WAL file \"%s\"", fname);
    else
        pg_fatal("could not find any WAL file");

    return NULL; // not reached
}
```