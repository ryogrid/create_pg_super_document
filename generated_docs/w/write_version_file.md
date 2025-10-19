# write_version_file

## Location
[src/bin/initdb/initdb.c:1019-1041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1019-L1041)

## Overview
A static utility function in initdb that creates the PG_VERSION file containing the PostgreSQL major version number in the data directory or its subdirectories.

## Definition

```c
static void
write_version_file(const char *extrapath)
```
## Detailed Description
The  function creates and writes the PG_VERSION file, which is essential for PostgreSQL to identify the version of a database cluster. The function constructs the appropriate file path by combining the global  directory with an optional subdirectory path. It opens the file in binary write mode and writes the major version number (from  macro) followed by a newline. This file is crucial for PostgreSQL's version compatibility checks and is created both in the main data directory and in individual tablespace directories during cluster initialization.

## Parameters / Member Variables
- `*extrapath`: An optional subdirectory path where the PG_VERSION file should be created. If NULL, the file is created directly in the main data directory.
## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (PostgreSQL's printf-like memory allocating function)
  - fopen (standard C library function for file opening)
  - fprintf (standard C library function for formatted output)
  - fclose (standard C library function for file closing)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error logging function)
  - free (standard C library function for memory deallocation)
  - PG_BINARY_W (PostgreSQL macro for binary write mode)
  - PG_MAJORVERSION (PostgreSQL macro containing the major version string)
  - pg_data (global variable containing the data directory path)
- Called from (representative examples):
  - [initialize_data_directory](../i/initialize_data_directory.md) (called twice - once for main directory, once for subdirectories)

## Notes and Other Information
- This is a static function, only accessible within initdb.c
- The function is fatal - it will terminate the program if file operations fail
- The PG_VERSION file is critical for PostgreSQL version compatibility checking
- Used during both main data directory initialization and tablespace creation
- The file contains only the major version number (e.g., "17") and a newline
- Memory allocated for the path string is properly freed after use
- Opens files in binary mode for cross-platform compatibility

## Simplified Source

```c
static void
write_version_file(const char *extrapath)
{
    FILE *version_file;
    char *path;

    // Construct file path - main data dir or subdirectory
    if (extrapath == NULL)
        path = psprintf("%s/PG_VERSION", pg_data);
    else
        path = psprintf("%s/%s/PG_VERSION", pg_data, extrapath);

    // Open file for writing
    if ((version_file = fopen(path, PG_BINARY_W)) == NULL)
        pg_fatal("could not open file \"%s\" for writing: %m", path);

    // Write major version number and close
    if (fprintf(version_file, "%s\n", PG_MAJORVERSION) < 0 || fclose(version_file))
        pg_fatal("could not write file \"%s\": %m", path);

    free(path);
}
```