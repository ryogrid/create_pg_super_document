# pg_tablespace_databases

## Location
src/backend/utils/adt/misc.c: 224 - 300

## Overview
Returns a set of database OIDs that have objects stored in a specified tablespace by examining the filesystem directories under the tablespace location.

## Definition


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
  - AllocateDir
  - ReadDir
  - FreeDir
  - [directory_is_empty](../d/directory_is_empty.md)
  - atooid
  - tuplestore_putvalues
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