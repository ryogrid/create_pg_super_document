# check_loadable_libraries

## Location
src/bin/pg_upgrade/function.c: 146 - 219

## Overview
Verifies that all required libraries from the old PostgreSQL cluster are present and compatible in the new cluster by attempting to LOAD each library.

## Definition
```c
void check_loadable_libraries(void)
```

## Detailed Description
This function performs a critical compatibility check during pg_upgrade by testing each library collected by get_loadable_libraries() in the new PostgreSQL installation. It connects to the template1 database in the new cluster and systematically attempts to execute LOAD commands for each unique library. The libraries are first sorted using library_name_compare() to ensure consistent ordering and avoid redundant probes. If any library fails to load, the function records the failure details in a loadable_libraries.txt file and terminates the upgrade process with a fatal error, providing guidance to the user on how to resolve missing library issues.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - connectToServer (connect to new cluster)
  - prep_status (status reporting)
  - qsort (sorting libraries)
  - library_name_compare (comparison function)
  - PQescapeStringConn, PQexec, PQclear, PQfinish (PostgreSQL operations)
  - fopen_priv (secure file opening)
  - pg_fatal, pg_log (error reporting)
  - check_ok (success reporting)
  - LibraryInfo (structure type)
  - PGRES_COMMAND_OK, PG_REPORT (constants)
- Called from (representative examples):
  - check_new_cluster

## Notes and Other Information
- Uses template1 database for testing library loads in the new cluster
- Eliminates duplicate library tests by comparing with the previous library name after sorting
- Creates loadable_libraries.txt file only when failures occur
- Provides detailed error messages including specific database names where libraries were referenced
- Terminates pg_upgrade process immediately upon detecting any missing libraries
- Sorting ensures reproducible behavior and proper dependency handling between libraries
- Uses PQescapeStringConn for safe SQL command construction with library paths