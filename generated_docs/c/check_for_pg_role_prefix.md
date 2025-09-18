# check_for_pg_role_prefix

## Location
[src/bin/pg_upgrade/check.c:1673-1727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1673-L1727)

## Overview
Validates that the old PostgreSQL cluster does not contain any user-created roles with names starting with "pg_", which is a reserved prefix for system roles.

## Definition
```c
static void check_for_pg_role_prefix(ClusterInfo *cluster)
```

## Detailed Description
This function checks for user-defined roles that inappropriately use the "pg_" prefix, which is reserved exclusively for PostgreSQL system roles. The function prevents cluster upgrades when such roles exist, as they could conflict with future system roles introduced in newer PostgreSQL versions.

The function performs the following operations:
- Connects to the template1 database in the cluster
- Queries pg_catalog.pg_roles for roles with names matching the "^pg_" pattern
- Writes any found roles to a report file with their OID and name
- Terminates the upgrade process with instructions to rename problematic roles

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being checked

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](connectToServer.md)
  - [prep_status](../p/prep_status.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - fopen_priv
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [pg_log](../p/pg_log.md)
  - [check_ok](check_ok.md)
- Called from:
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Creates an output file "pg_role_prefix.txt" in the log base directory when problematic roles are found
- Uses a regular expression "^pg_" to match roles starting with the reserved prefix
- The check applies specifically to versions older than 9.6 where such roles should not exist
- Provides clear guidance that roles must be renamed before upgrade can proceed
- Part of PostgreSQL's upgrade validation to prevent naming conflicts with future system roles