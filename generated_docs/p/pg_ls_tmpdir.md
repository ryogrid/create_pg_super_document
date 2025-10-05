# pg_ls_tmpdir

## Location
[src/backend/utils/adt/genfile.c:649-667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L649-L667)

## Overview
A generic internal function that lists files in the temporary directory (pgsql_tmp) for a specified tablespace.

## Definition

```c
static Datum
pg_ls_tmpdir(FunctionCallInfo fcinfo, Oid tblspc)
```
## Detailed Description
This function provides the core functionality for listing files in PostgreSQL's temporary directory structure. It validates that the specified tablespace exists in the system catalog, constructs the path to the temporary directory for that tablespace, and delegates the actual file listing to the generic pg_ls_dir_files function. The function is designed to be called by wrapper functions that provide different parameter interfaces for SQL functions.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing context and parameters for the function call
- `tblspc`: Object ID (Oid) of the tablespace whose temporary directory should be listed
## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists1 (validates tablespace existence)
  - [TempTablespacePath](../T/TempTablespacePath.md) (constructs temporary directory path)
  - [pg_ls_dir_files](pg_ls_dir_files.md) (performs the actual directory listing)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (parameter structure type)
- Called from (representative examples):
  - [pg_ls_tmpdir_noargs](pg_ls_tmpdir_noargs.md)
  - [pg_ls_tmpdir_1arg](pg_ls_tmpdir_1arg.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the genfile.c compilation unit
- The function throws an error if the specified tablespace OID doesn't exist in the system catalog
- Returns the result of pg_ls_dir_files with the show_size parameter set to true
- Part of PostgreSQL's file system inspection functionality accessible via SQL functions

## Simplified Source

```c
static Datum
pg_ls_tmpdir(FunctionCallInfo fcinfo, Oid tblspc)
{
    char path[MAXPGPATH];

    // Validate that the tablespace exists
    if (!SearchSysCacheExists1(TABLESPACEOID, ObjectIdGetDatum(tblspc)))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("tablespace with OID %u does not exist", tblspc)));

    // Construct path to tablespace's temporary directory
    TempTablespacePath(path, tblspc);

    // List files in the temporary directory (missing_ok = true)
    return pg_ls_dir_files(fcinfo, path, true);
}
```