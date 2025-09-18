# vacuum_all_databases

## Location
src/bin/scripts/vacuumdb.c: 909 - 975

## Overview
Orchestrates vacuum and analyze operations across all connectable databases in a PostgreSQL cluster, with support for staged analyze operations to ensure minimal statistics are available quickly across all databases.

## Definition
```c
static void vacuum_all_databases(ConnParams *cparams,
                                 vacuumingOptions *vacopts,
                                 bool analyze_in_stages,
                                 SimpleStringList *objects,
                                 int concurrentCons,
                                 const char *progname, bool echo, bool quiet)
```

## Detailed Description
This function implements the "all databases" functionality of the vacuumdb utility, systematically processing every database in a PostgreSQL cluster that allows connections and is not disabled. It queries the system catalogs to discover all accessible databases and then delegates the actual vacuum/analyze work to the `vacuum_one_database` function for each database.

The function supports two distinct operational modes:

1. **Standard mode**: Processes each database completely before moving to the next database
2. **Analyze-in-stages mode**: Processes all databases in the fastest analyze stage first, then moves to progressively more detailed stages across all databases

The staged approach ensures that basic optimizer statistics become available across all databases as quickly as possible, which is particularly beneficial in environments with many databases where users want to ensure some level of statistics are available everywhere before investing time in more detailed analysis.

## Parameters / Member Variables
- `cparams`: Database connection parameters structure (dbname will be overridden for each database)
- `vacopts`: Structure containing all vacuuming options and flags to apply to each database
- `analyze_in_stages`: Whether to use staged analyze mode across all databases
- `objects`: List of user-specified tables/schemas to process in each database (can be NULL)
- `concurrentCons`: Number of concurrent connections to use per database
- `progname`: Program name for error reporting
- `echo`: Whether to echo SQL commands being executed
- `quiet`: Whether to suppress progress messages

## Dependencies
- Functions called/Symbols referenced:
  - connectMaintenanceDatabase (establish connection to maintenance database)
  - executeQuery (execute catalog query to find databases)
  - vacuum_one_database (process individual databases)
  - PQfinish/PQclear (cleanup database connections and results)
  - ANALYZE_NUM_STAGES/ANALYZE_NO_STAGE (stage constants)
- Called from (representative examples):
  - main (vacuumdb main function when --all flag is used)

## Notes and Other Information
- Queries pg_database to find all databases where datallowconn is true and datconnlimit is not -2 (not disabled)
- Results are ordered by database name for consistent processing order
- In staged mode, establishes multiple times as many database connections but ensures faster availability of basic statistics
- Uses the override_dbname field in cparams to specify which database to connect to for each iteration
- The function is marked static, indicating internal use within vacuumdb.c only
- Does not process template databases or databases that explicitly disallow connections
- Each database is processed independently, so failures in one database do not affect processing of others