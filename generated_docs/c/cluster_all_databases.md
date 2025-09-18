# cluster_all_databases

## Location
src/bin/scripts/clusterdb.c: 228 - 271

## Overview
Iterates through all accessible databases in a PostgreSQL cluster and performs clustering operations on each one, either for specific tables or all eligible tables.

## Definition
```c
static void cluster_all_databases(ConnParams *cparams, SimpleStringList *tables,
                                 const char *progname, bool verbose, bool echo, 
                                 bool quiet)
```

## Detailed Description
This function implements the database-wide clustering functionality for the clusterdb utility. It connects to the maintenance database to query pg_database for all databases that allow connections and are not templates (datconnlimit <> -2). For each accessible database, it calls cluster_one_database to perform the actual clustering. If a specific table list is provided, it clusters only those tables in each database; otherwise, it clusters all eligible tables. The function provides progress feedback unless running in quiet mode.

## Parameters / Member Variables
- `cparams`: Connection parameters structure, with override_dbname modified for each database
- `tables`: List of specific tables to cluster, or empty list to cluster all eligible tables
- `progname`: Program name used for progress messages and error reporting
- `verbose`: Passed to cluster_one_database for detailed CLUSTER command output
- `echo`: Passed to cluster_one_database for SQL command echoing
- `quiet`: If true, suppresses progress messages during execution

## Dependencies
- Functions called/Symbols referenced:
  - connectMaintenanceDatabase
  - executeQuery
  - PQfinish
  - PQntuples
  - PQgetvalue
  - printf
  - fflush
  - cluster_one_database
  - PQclear
- Called from:
  - main (in clusterdb.c)

## Dependencies
- Functions called/Symbols referenced:
  - ConnParams
  - SimpleStringList
  - SimpleStringListCell

## Notes and Other Information
- Part of the clusterdb utility when --all-databases option is used
- Dynamically modifies connection parameters to target each database
- Queries system catalog pg_database to discover available databases
- Excludes template databases and those that don't allow connections
- Provides user-friendly progress feedback showing current database being processed
- Handles both specific table lists and database-wide clustering scenarios