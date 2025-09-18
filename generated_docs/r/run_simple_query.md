# run_simple_query

## Location
src/bin/pg_rewind/libpq_source.c: 164 - 191

## Overview
Executes a SQL query that is expected to return a single string value and returns that value as a dynamically allocated string.

## Definition


## Detailed Description
The  function is a utility function that executes a SQL query on a PostgreSQL connection and expects exactly one row and one column in the result set. It performs strict validation of the result set format and returns the single value as a newly allocated string.

The function includes comprehensive error checking:
1. Verifies the query executed successfully (PGRES_TUPLES_OK status)
2. Validates the result set contains exactly one field (column) and one tuple (row)
3. Ensures the returned value is not NULL
4. Creates a copy of the result string using 

This function is designed for queries that return configuration values, status information, or other single-value results where the exact format is predictable and critical for correct operation.

## Parameters
- : PostgreSQL connection to execute the query on
- : SQL query string that should return exactly one row and one column

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [PQnfields](../P/PQnfields.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [pg_fatal](../p/pg_fatal.md)
  - PGRES_TUPLES_OK
- Called from:
  - [init_libpq_conn](../i/init_libpq_conn.md) (in src/bin/pg_rewind/libpq_source.c:140)
  - [libpq_get_current_wal_insert_lsn](../l/libpq_get_current_wal_insert_lsn.md) (in src/bin/pg_rewind/libpq_source.c:217)

## Notes and Other Information
- This is a static function, only accessible within the libpq_source.c file
- The returned string must be freed by the caller using  to prevent memory leaks
- The function will terminate the program with  if the query fails or returns an unexpected result format
- Commonly used for queries like 'SHOW full_page_writes' or functions that return single values like LSN positions
- The strict validation ensures that programming errors or unexpected server responses are caught immediately