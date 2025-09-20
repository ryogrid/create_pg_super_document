# check_data_dir

## Location
[src/bin/pg_upgrade/exec.c:341-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/exec.c#L341-L382)

## Overview
Validates a PostgreSQL cluster data directory by verifying the presence of essential subdirectories required for a valid $PGDATA directory.

## Definition

```c
struct stat statBuf;
```
## Detailed Description
This function performs comprehensive validation of a PostgreSQL cluster's data directory structure. It ensures that all required subdirectories exist and are accessible, which is critical for the pg_upgrade process to function correctly. The function also retrieves the cluster's major version and handles version-specific directory names that changed between PostgreSQL versions.

The function checks for the presence of core PostgreSQL directories including base (database files), global (cluster-wide files), and various transaction log directories. It adapts to PostgreSQL version changes, specifically handling the renaming of pg_xlog to pg_wal and pg_clog to pg_xact in PostgreSQL 10.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing cluster configuration including pgdata path and version information

## Dependencies
- Functions called/Symbols referenced:
  - [get_major_server_version](../g/get_major_server_version.md)
  - [check_single_dir](check_single_dir.md)
  - GET_MAJOR_VERSION
- Called from (representative examples):
  - [verify_directories](../v/verify_directories.md)

## Notes and Other Information
- Exits the program with an error message if any required directory is missing or inaccessible
- Handles PostgreSQL version-specific directory naming changes (v10+ renames)
- Essential for ensuring data directory integrity before upgrade operations
- Part of the pg_upgrade utility's pre-flight validation process