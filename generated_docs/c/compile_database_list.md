# compile_database_list

## Location
src/bin/pg_amcheck/pg_amcheck.c: 1583 - 1774

## Overview
Compiles a distinct list of PostgreSQL databases to check based on user-specified patterns and command-line options in the pg_amcheck utility.

## Definition


## Detailed Description
This function constructs a comprehensive list of databases to be checked by pg_amcheck. It handles various scenarios: explicit database patterns provided by the user, the --all flag for checking all databases, inclusion/exclusion pattern matching, and ensures proper filtering of connectable databases. The function uses a complex SQL query with multiple CTEs (Common Table Expressions) to efficiently resolve patterns against the pg_database catalog, applying inclusion and exclusion rules while respecting database connectivity constraints.

## Parameters / Member Variables  
- `conn`: PostgreSQL connection handle to the initial database for executing pattern resolution queries
- `databases`: Pointer to SimplePtrList structure that will be populated with DatabaseInfo objects representing databases to check
- `initial_dbname`: Optional initial database name to unconditionally include in the list (typically the connection database)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0
  - pg_log_info  
  - [pstrdup](../p/pstrdup.md)
  - [simple_ptr_list_append](../s/simple_ptr_list_append.md)
  - initPQExpBuffer
  - [append_db_pattern_cte](../a/append_db_pattern_cte.md)
  - termPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - pg_log_error
  - pg_log_error_detail
  - [disconnectDatabase](../d/disconnectDatabase.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - log_no_match
  - [PQclear](../P/PQclear.md)
  - [DatabaseInfo](../D/DatabaseInfo.md)
  - [SimplePtrList](../S/SimplePtrList.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:499)
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:513)

## Notes and Other Information
- Constructs a complex SQL query using multiple CTEs: include_raw, exclude_raw, database, include_pat, and filtered_databases
- Handles edge case where no database patterns exist and --all is not specified, avoiding unnecessary database queries
- Filters out non-connectable databases (datallowconn=false or datconnlimit=-2)
- Supports strict name checking mode where unmatched patterns cause fatal errors
- Prevents duplicate entries when initial_dbname matches a pattern-resolved database
- The generated SQL query efficiently combines inclusion/exclusion logic with database connectivity filtering
- Critical component of pg_amcheck's database discovery mechanism for pattern-based database selection