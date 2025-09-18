# executeQueryOrDie

## Location
src/bin/pg_upgrade/server.c: 122 - 158

## Overview
Executes SQL queries with formatted string support and automatic program termination on query failure.

## Definition
```c
PGresult *executeQueryOrDie(PGconn *conn, const char *fmt, ...)
```

## Detailed Description
This variadic function formats and executes SQL queries using printf-style formatting. It provides comprehensive error handling with verbose logging and implements a "fail-fast" approach - any query execution failure results in immediate program termination. The function logs the executed query at verbose level and checks the result status against both PGRES_TUPLES_OK and PGRES_COMMAND_OK for success determination.

## Parameters / Member Variables
- `conn`: Active PostgreSQL database connection
- `fmt`: Printf-style format string for the SQL query
- `...`: Variable arguments corresponding to format specifiers

## Dependencies
- Functions called/Symbols referenced:
  - va_start
  - vsnprintf
  - va_end
  - pg_log
  - PQexec
  - PQresultStatus
  - PQerrorMessage
  - PQclear
  - PQfinish
- Called from (representative examples):
  - connectToServer
  - check_for_data_types_usage
  - check_is_install_user
  - get_template0_info
  - get_db_infos
  - set_locale_and_encoding

## Notes and Other Information
- Uses static buffer (QUERY_ALLOC size) for query formatting - queries exceeding this limit may be truncated
- Implements "fail-fast" pattern consistent with other pg_upgrade utilities
- Accepts both SELECT queries (PGRES_TUPLES_OK) and modification queries (PGRES_COMMAND_OK)
- Provides verbose logging for debugging and troubleshooting upgrade operations
- Central query execution point for most pg_upgrade database operations
- Automatically cleans up resources (result and connection) on failure before termination