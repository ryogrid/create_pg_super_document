# check_for_tables_with_oids

## Location
src/bin/pg_upgrade/check.c: 1519 - 1595

## Overview
Detects tables declared WITH OIDS and blocks PostgreSQL upgrades since OID system columns are no longer supported in modern PostgreSQL versions.

## Definition
```c
static void check_for_tables_with_oids(ClusterInfo *cluster)
```

## Detailed Description
This function enforces compatibility by preventing upgrades when user tables with OID system columns exist in the source cluster. OIDs (Object Identifiers) were deprecated and removed as a table-level feature in PostgreSQL 12 due to performance overhead, maintenance complexity, and limited utility in most applications.

The function systematically scans all databases in the cluster, querying the pg_class system catalog to identify any user tables that have the relhasoids flag set to true. It specifically excludes system catalogs (pg_catalog schema) since these may legitimately use OIDs internally and are handled separately by the upgrade process.

When tables with OIDs are found, the upgrade process is halted and provides clear guidance to users on how to resolve the issue using the ALTER TABLE ... SET WITHOUT OIDS command. This allows users to remove OID columns manually before retrying the upgrade.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being validated

## Dependencies
- Functions called/Symbols referenced:
  - prep_status - Updates status display for the validation operation
  - connectToServer - Establishes connections to each database in the cluster
  - executeQueryOrDie - Executes SQL query to find tables with OID columns
  - fopen_priv - Opens output file with proper permissions for logging problematic tables
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
- Location: src/bin/pg_upgrade/check.c:1519-1595
- The SQL query uses relhasoids column from pg_class to identify tables with OID system columns
- Excludes pg_catalog schema tables as these are system-managed and handled differently during upgrades
- When issues are detected, problematic tables are logged to 'tables_with_oids.txt' in the log directory
- The error message provides specific remediation guidance using ALTER TABLE ... SET WITHOUT OIDS
- This validation ensures that deprecated table features don't cause compatibility issues in newer PostgreSQL versions
- OIDs were originally intended as unique row identifiers but are now considered obsolete in favor of proper primary keys and sequences