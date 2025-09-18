# check_for_user_defined_postfix_ops

## Location
src/bin/pg_upgrade/check.c: 1295 - 1392

## Overview
Detects and blocks PostgreSQL upgrades when user-defined postfix operators are present, as these operators are no longer supported in newer PostgreSQL versions.

## Definition
```c
static void check_for_user_defined_postfix_ops(ClusterInfo *cluster)
```

## Detailed Description
This function enforces compatibility by preventing upgrades when user-defined postfix operators exist in the source cluster. Postfix operators (operators that appear after their operand, like 'value++') were deprecated and eventually removed from PostgreSQL due to parsing ambiguities and maintenance complexity.

The function systematically scans all databases in the cluster, querying the pg_operator system catalog to find any postfix operators (identified by oprright = 0, indicating no right operand) that have OIDs >= 16384, which indicates they are user-defined rather than built-in system operators. The hardcoded value 16384 represents FirstNormalObjectId and ensures the check remains consistent even if this constant changes in future PostgreSQL versions.

When postfix operators are found, the upgrade process is halted and detailed information about each operator (including OID, namespace, name, and operand type) is logged to help users identify and migrate away from these unsupported constructs.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being validated

## Dependencies
- Functions called/Symbols referenced:
  - prep_status - Updates status display for the validation operation
  - connectToServer - Establishes connections to each database in the cluster
  - executeQueryOrDie - Executes SQL query to find user-defined postfix operators
  - fopen_priv - Opens output file with proper permissions for logging problematic operators
  - PQntuples, PQfnumber, PQgetvalue - PostgreSQL result set processing functions
  - PQclear - Releases PostgreSQL result set memory
  - PQfinish - Closes database connections
  - pg_log - Logs messages at specified severity level
  - pg_fatal - Terminates upgrade process with fatal error message
  - check_ok - Marks validation as successful when no issues are found
- Called from (representative examples):
  - check_and_dump_old_cluster - Part of old cluster validation sequence

## Notes and Other Information
- This is a static function within the pg_upgrade check.c module
- Location: src/bin/pg_upgrade/check.c:1295-1392
- The function uses a hardcoded FirstNormalObjectId value (16384) rather than interpolating the C #define to maintain consistency with pre-version 14 behavior
- When issues are detected, operators are logged to a file named 'postfix_ops.txt' in the log directory
- The SQL query specifically looks for operators where oprright = 0 (no right operand) and oid >= 16384 (user-defined)
- Users encountering this error must manually convert postfix operators to prefix operators or function calls before upgrading
- This validation helps ensure that deprecated language features don't cause issues in newer PostgreSQL versions