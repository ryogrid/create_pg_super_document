# pg_ls_dir

## Location
[src/backend/utils/adt/genfile.c:498-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L498-L557)

## Overview
Lists the contents of a directory and returns only the filenames as a set-returning function, with optional parameters to handle missing directories and include dot directories.

## Definition

```c
struct dirent *de;
```
## Detailed Description
The  function is a PostgreSQL system function that provides directory listing functionality accessible from SQL. It reads the contents of a specified directory and returns the filenames as a result set. The function supports up to 3 arguments: the directory path (required), a missing_ok flag (optional), and an include_dot_dirs flag (optional).

The function uses PostgreSQL's set-returning function (SRF) infrastructure to return multiple rows, with each row containing a single filename from the directory. It performs proper error handling for missing directories and can optionally filter out dot directories (. and ..).

## Parameters / Member Variables
- : Directory path to list (converted and validated)
- : missing_ok - if true, returns empty result instead of error when directory doesn't exist
- : include_dot_dirs - if true, includes "." and ".." entries in the results

## Dependencies
- Functions called/Symbols referenced:
  - [convert_and_check_filename](../c/convert_and_check_filename.md) (validates and converts directory path)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initializes set-returning function infrastructure)
  - [AllocateDir](../A/AllocateDir.md) (opens directory for reading)
  - [ReadDir](../R/ReadDir.md) (reads directory entries)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md) (adds result rows to output)
  - [FreeDir](../F/FreeDir.md) (closes directory handle)
  - [ReturnSetInfo](../R/ReturnSetInfo.md), DIR, dirent (data structures)
  - MAT_SRF_USE_EXPECTED_DESC (SRF configuration constant)
- Called from (representative examples):
  - [pg_ls_dir_1arg](pg_ls_dir_1arg.md) (wrapper function with fewer parameters)

## Notes and Other Information
- The function validates directory paths through convert_and_check_filename to prevent directory traversal attacks
- Uses PostgreSQL's materialized SRF pattern for efficient result set generation
- Proper resource management with AllocateDir/FreeDir pairing
- Error handling respects the missing_ok parameter to provide graceful degradation
- Dot directory filtering is performed at the application level rather than filesystem level
- Returns empty result set (not NULL) when directory is missing and missing_ok is true

## Simplified Source

```c
Datum pg_ls_dir(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    bool missing_ok = false;
    bool include_dot_dirs = false;

    // Convert and validate directory path
    char *location = convert_and_check_filename(PG_GETARG_TEXT_PP(0));

    // Parse optional arguments (missing_ok and include_dot_dirs)
    if (PG_NARGS() == 3) {
        if (!PG_ARGISNULL(1))
            missing_ok = PG_GETARG_BOOL(1);
        if (!PG_ARGISNULL(2))
            include_dot_dirs = PG_GETARG_BOOL(2);
    }

    // Initialize set-returning function infrastructure
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

    // Open directory
    DIR *dirdesc = AllocateDir(location);
    if (!dirdesc) {
        // Return empty result if missing_ok is true
        if (missing_ok && errno == ENOENT)
            return (Datum) 0;
        // Otherwise let ReadDir() handle the error
    }

    // Read directory entries
    struct dirent *de;
    while ((de = ReadDir(dirdesc, location)) != NULL) {
        // Skip dot directories unless requested
        if (!include_dot_dirs &&
            (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0))
            continue;

        // Add filename to result set
        Datum values[1];
        bool nulls[1];
        values[0] = CStringGetTextDatum(de->d_name);
        nulls[0] = false;

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    FreeDir(dirdesc);
    return (Datum) 0;
}
```