# check_for_isn_and_int8_passing_mismatch

## Location
[src/bin/pg_upgrade/check.c:1214-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1214-L1294)

## Overview
Validates compatibility between old and new PostgreSQL clusters when the contrib/isn extension is present and there are differences in bigint (int8) passing mechanisms.

## Definition
```c
static void check_for_isn_and_int8_passing_mismatch(ClusterInfo *cluster)
```

## Detailed Description
This function addresses a specific compatibility issue that arose in PostgreSQL 8.4 when the int8 (bigint) data type changed from being passed by reference to being passed by value on 64-bit platforms. The contrib/isn extension relies heavily on the int8 data type, and this change in passing mechanism creates binary incompatibilities between PostgreSQL versions.

The function first compares the float8_pass_by_value settings between the old and new clusters. If they match, no issue exists and the check passes. However, if there's a mismatch, the function scans all databases in the cluster to identify any functions from the contrib/isn extension (identified by their probin path '/isn'). 

If contrib/isn functions are found when there's a passing mechanism mismatch, the upgrade is blocked and the user must manually handle the affected databases by dumping them, dropping the isn functions, performing the upgrade, and then restoring the databases.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being validated

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) - Updates status display for the validation operation
  - [check_ok](check_ok.md) - Marks the validation as successful when no issues are found
  - [connectToServer](connectToServer.md) - Establishes connections to each database in the cluster
  - [executeQueryOrDie](../e/executeQueryOrDie.md) - Executes SQL query to find contrib/isn functions
  - fopen_priv - Opens output file with proper permissions for logging problematic functions
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue - PostgreSQL result set processing functions
  - [PQclear](../P/PQclear.md) - Releases PostgreSQL result set memory
  - [PQfinish](../P/PQfinish.md) - Closes database connections
  - [pg_log](../p/pg_log.md) - Logs messages at specified severity level
  - [pg_fatal](../p/pg_fatal.md) - Terminates upgrade process with fatal error message
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md) - Part of old cluster validation sequence

## Notes and Other Information
- This is a static function within the pg_upgrade check.c module
- Location: src/bin/pg_upgrade/check.c:1214-1294
- The function was introduced to handle a specific compatibility issue between PostgreSQL versions regarding the contrib/isn extension
- When issues are detected, problematic functions are logged to a file named 'contrib_isn_and_int8_pass_by_value.txt' in the log directory
- The check specifically looks for functions with probin = '/isn' to identify contrib/isn extension usage
- This validation only runs on the old cluster, as indicated by its single call site in check_and_dump_old_cluster