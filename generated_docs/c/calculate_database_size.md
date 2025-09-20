# calculate_database_size

## Location
[src/backend/utils/adt/dbsize.c:118-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L118-L167)

## Overview
A static function that calculates the total physical size of a database across all tablespaces, including both the default tablespace (pg_default) and any user-defined tablespaces.

## Definition

```c
struct dirent *direntry;
```
## Detailed Description
The  function computes the total size of a database by scanning all tablespaces where the database has objects stored. It first performs privilege checking to ensure the user has either CONNECT privilege on the target database or has the pg_read_all_stats role. The function then calculates the size in two phases: first, it measures the size of the database's directory in the default tablespace (base/dbOid), then it iterates through all user-defined tablespaces in pg_tblspc and adds the size of the database's directory in each tablespace. The function excludes shared storage in pg_global from the calculation, as this storage is not database-specific.

## Parameters / Member Variables
- : The OID (Object Identifier) of the database whose size should be calculated

## Dependencies
- Functions called/Symbols referenced:
  - : Checks access control permissions for the database
  - : Checks if user has privileges of a specific role
  - : Reports access control errors
  - : Gets the name of a database from its OID
  - : Calculates the size of a directory (called multiple times)
  - : Opens a directory for reading
  - : Reads directory entries
  - : Closes and frees directory resources
  - : Allows query cancellation
  - : Access control constant for connect privilege
  - : Role constant for read-all-stats privilege
  - : Object type constant for databases
  - : Directory name for tablespace versions
- Called from (representative examples):
  - : Public function that takes a database OID
  - : Public function that takes a database name

## Notes and Other Information
- This is a static function, only accessible within dbsize.c
- Performs comprehensive access control checking before calculating sizes
- Excludes pg_global tablespace as it contains shared objects not specific to any database
- Uses the database's OID to construct paths in both default and custom tablespaces
- The function handles interruption checking during tablespace scanning for long operations
- Total size includes all database files across all tablespaces where the database has objects
- [Path](../P/Path.md) construction follows PostgreSQL's internal directory structure: base/dbOid for default, pg_tblspc/tblspc_oid/version_dir/dbOid for custom tablespaces