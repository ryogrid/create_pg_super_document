# server_is_in_recovery

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:813-840](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L813-L840)

## Overview
Checks whether the PostgreSQL server is currently in recovery mode by querying the pg_is_in_recovery() system function.

## Definition

```c
static bool
server_is_in_recovery(PGconn *conn)
```
## Detailed Description
This function determines if a PostgreSQL server is in recovery mode by executing a SQL query against the pg_catalog.pg_is_in_recovery() system function. It handles the result by comparing the returned string value ('t' for true, 'f' for false) to determine the recovery status. The function provides error handling for query execution failures and properly cleans up resources.

## Parameters / Member Variables
- : PostgreSQL database connection handle used to execute the recovery status query

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md)
  - PGRES_TUPLES_OK
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [disconnect_database](../d/disconnect_database.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - strcmp
- Called from (representative examples):
  - [check_publisher](../c/check_publisher.md)
  - [check_subscriber](../c/check_subscriber.md)
  - [wait_for_end_recovery](../w/wait_for_end_recovery.md)

## Notes and Other Information
- Returns true if the server is in recovery mode, false otherwise
- Uses string comparison with 't' to determine boolean result from SQL query
- Terminates the program if the query fails by calling disconnect_database with exit flag
- Essential for determining when a standby server has completed recovery and can be promoted