# get_tablespace_paths

## Location
[src/bin/pg_upgrade/tablespace.c:40-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/tablespace.c#L40-L102)

## Overview
Scans the pg_tablespace system catalog and retrieves all user-defined tablespace paths, validating their existence and accessibility for pg_upgrade operations.

## Definition
static void get_tablespace_paths(void)

## Detailed Description
The get_tablespace_paths function is a critical component of the pg_upgrade utility that discovers and validates all user-defined tablespaces in the old cluster. It performs several key operations:

1. **Tablespace Discovery**: Connects to the old cluster's template1 database and queries pg_tablespace to find all user-defined tablespaces (excluding pg_default and pg_global)
2. **Path Extraction**: Uses pg_catalog.pg_tablespace_location(oid) to get the actual filesystem paths for each tablespace
3. **Memory Allocation**: Dynamically allocates memory to store the tablespace paths in the global os_info structure
4. **Path Validation**: For each discovered tablespace, performs filesystem checks to ensure:
   - The path exists on the filesystem
   - The path points to a directory (not a file or other filesystem object)
   - The directory is accessible

The function is essential for ensuring that all tablespace dependencies are properly identified and validated before attempting a database upgrade. It prevents upgrade failures that could occur due to missing or inaccessible tablespace directories.

## Parameters / Member Variables
This function takes no parameters but modifies global state:
- Updates  with the count of discovered tablespaces
- Populates  array with tablespace paths

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [stat](../s/stat.md)
  - S_ISDIR
  - report_status
- Called from (representative examples):
  - [init_tablespaces](../i/init_tablespaces.md) (src/bin/pg_upgrade/tablespace.c:21)

## Notes and Other Information
- This is a static function, only accessible within the tablespace.c compilation unit
- The function performs fatal error reporting if any tablespace directory is missing or inaccessible
- Memory allocated for tablespace paths must be freed by the caller
- The validation checks help catch common issues where tablespace symbolic links become broken during cluster migration preparation
- Only user-defined tablespaces are processed; system tablespaces (pg_default, pg_global) are excluded from the query
- The function uses template1 database for connection, ensuring it can connect even if user databases have issues