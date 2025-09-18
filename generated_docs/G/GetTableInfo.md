# GetTableInfo

## Location
[src/bin/pgbench/pgbench.c:5344-5452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5344-L5452)

## Overview
Extracts pgbench table information from the database to determine the scaling factor and partitioning configuration by querying the existing pgbench tables.

## Definition
```c
static void GetTableInfo(PGconn *con, bool scale_given)
```

## Detailed Description
This function gathers essential information about the existing pgbench database setup by querying system tables. It performs two main operations:

1. **Scale Factor Detection**: Queries the pgbench_branches table to count the number of branches, which determines the scale factor used during database initialization. This value is stored in the global  variable.

2. **Partition Information Discovery**: Examines the pgbench_accounts table to determine if it is partitioned and, if so, what partitioning method is used and how many partitions exist. This information is stored in global variables  and .

The function handles various error conditions gracefully, including missing tables (suggesting initialization is needed) and older PostgreSQL versions that don't support partitioning. For partitioned tables, it supports both range and hash partitioning methods.

If a scale factor was provided via command line but differs from the database contents, the function issues a warning and uses the database value instead.

## Parameters / Member Variables
- `con`: PostgreSQL connection handle for database queries
- `scale_given`: Boolean indicating whether the user specified a scale factor via command line (used for warning generation)

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) (execute SQL queries)
  - [PQresultStatus](../P/PQresultStatus.md) (check query result status)
  - PGRES_TUPLES_OK (successful query result constant)
  - [PQresultErrorField](../P/PQresultErrorField.md) (extract error details)
  - PG_DIAG_SQLSTATE (SQL state error field)
  - ERRCODE_UNDEFINED_TABLE (undefined table error code)
  - pg_log_error (error logging)
  - pg_log_error_hint (error hint logging)
  - pg_log_warning (warning logging)
  - [PQdb](../P/PQdb.md) (get database name from connection)
  - [PQgetvalue](../P/PQgetvalue.md) (extract result values)
  - [PQntuples](../P/PQntuples.md) (get number of result rows)
  - [PQgetisnull](../P/PQgetisnull.md) (check for NULL values)
  - [PQclear](../P/PQclear.md) (free result memory)
  - atoi (convert string to integer)
  - PART_NONE, PART_RANGE, PART_HASH (partitioning method constants)
  - Assert (debugging assertion)
- Called from (representative examples):
  - [main](../m/main.md) (during pgbench setup phase)

## Notes and Other Information
- Sets global variables: , , and 
- Provides helpful error hints suggesting database initialization when tables are missing
- Handles older PostgreSQL versions gracefully by assuming no partitioning on query failure
- The partitioning query uses a complex join to find the first pgbench_accounts table in the search_path
- Supports detection of range ('r') and hash ('h') partitioning methods
- Issues warnings when user-provided scale factors are overridden by database contents
- Exits the program on critical errors (missing tables, invalid data)
- The scale factor represents the number of branches and is used to calculate other table sizes
- Located in src/bin/pgbench/pgbench.c:5344-5452