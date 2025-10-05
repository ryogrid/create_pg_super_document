# pg_tablespace_databases

## Location
[src/backend/utils/adt/misc.c:224-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L224-L300)

## Overview
Returns a set of database OIDs that have objects stored in a specified tablespace by examining the filesystem directories under the tablespace location.

## Definition

```c
Datum pg_tablespace_databases(PG_FUNCTION_ARGS)
```
## Detailed Description
This function takes a tablespace OID as input and returns a set of database OIDs that actually use storage space in that tablespace. It works by:

1. Converting the tablespace OID to the corresponding filesystem directory path
2. Scanning the directory for database subdirectories (named by their OIDs)
3. Checking if each database subdirectory contains any files (non-empty)
4. Returning only the OIDs of databases that have non-empty subdirectories

The function handles special cases:
- GLOBALTABLESPACE_OID: Returns warning and empty set (global objects don't belong to databases)
- DEFAULTTABLESPACE_OID: Maps to "base" directory
- Other tablespaces: Maps to "pg_tblspc/{oid}/PG_{major}_{catalog_version_no}" directory

If the tablespace directory doesn't exist, it issues a warning and returns an empty set.

## Parameters / Member Variables
- : The OID of the tablespace to examine (retrieved via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md)
  - [FreeDir](../F/FreeDir.md)
  - [directory_is_empty](../d/directory_is_empty.md)
  - atooid
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md)
  - [psprintf](psprintf.md)
- Constants referenced:
  - GLOBALTABLESPACE_OID
  - DEFAULTTABLESPACE_OID
  - TABLESPACE_VERSION_DIRECTORY
  - MAT_SRF_USE_EXPECTED_DESC
- Called from:
  - SQL function calls (no direct C references found)

## Notes and Other Information
- This function is typically used by system administration queries to determine tablespace usage
- The function filters out empty database directories to avoid reporting unused allocations
- Error handling includes directory access failures and invalid tablespace OIDs
- Returns data as a set-returning function (SRF) using the materialized approach
- The directory scanning approach makes this function filesystem-dependent and potentially expensive for large tablespaces

## Simplified Source

```c
Datum
pg_tablespace_databases(PG_FUNCTION_ARGS)
{
    Oid tablespaceOid = PG_GETARG_OID(0);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    char *location;
    DIR *dirdesc;
    struct dirent *de;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

    // Handle special tablespace cases
    if (tablespaceOid == GLOBALTABLESPACE_OID) {
        ereport(WARNING, (errmsg("global tablespace never has databases")));
        return (Datum) 0;
    }

    // Determine tablespace directory path
    if (tablespaceOid == DEFAULTTABLESPACE_OID)
        location = "base";
    else
        location = psprintf("pg_tblspc/%u/%s", tablespaceOid,
                           TABLESPACE_VERSION_DIRECTORY);

    // Open tablespace directory
    dirdesc = AllocateDir(location);
    if (!dirdesc) {
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not open directory \"%s\": %m", location)));
        ereport(WARNING, (errmsg("%u is not a tablespace OID", tablespaceOid)));
        return (Datum) 0;
    }

    // Scan directory for database subdirs
    while ((de = ReadDir(dirdesc, location)) != NULL) {
        Oid datOid = atooid(de->d_name);
        char *subdir;
        bool isempty;
        Datum values[1];
        bool nulls[1];

        // Skip non-numeric entries (., .., etc.)
        if (!datOid)
            continue;

        // Check if database subdir has any files
        subdir = psprintf("%s/%s", location, de->d_name);
        isempty = directory_is_empty(subdir);
        pfree(subdir);

        if (isempty)
            continue;

        // Return database OID for non-empty subdirs
        values[0] = ObjectIdGetDatum(datOid);
        nulls[0] = false;

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
                            values, nulls);
    }

    FreeDir(dirdesc);
    return (Datum) 0;
}
```