# server_is_in_recovery

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 813 - 840

## Overview
Checks whether the PostgreSQL server is currently in recovery mode by querying the pg_is_in_recovery() system function.

## Definition


## Detailed Description
This function determines if a PostgreSQL server is in recovery mode by executing a SQL query against the pg_catalog.pg_is_in_recovery() system function. It handles the result by comparing the returned string value ('t' for true, 'f' for false) to determine the recovery status. The function provides error handling for query execution failures and properly cleans up resources.

## Parameters / Member Variables
- : PostgreSQL database connection handle used to execute the recovery status query

## Dependencies
- Functions called/Symbols referenced:
  - PQexec
  - PGRES_TUPLES_OK
  - PQresultErrorMessage
  - disconnect_database
  - PQgetvalue
  - PQclear
  - strcmp
- Called from (representative examples):
  - check_publisher
  - check_subscriber
  - wait_for_end_recovery

## Notes and Other Information
- Returns true if the server is in recovery mode, false otherwise
- Uses string comparison with 't' to determine boolean result from SQL query
- Terminates the program if the query fails by calling disconnect_database with exit flag
- Essential for determining when a standby server has completed recovery and can be promoted