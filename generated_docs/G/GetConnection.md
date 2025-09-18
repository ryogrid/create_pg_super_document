# GetConnection

## Location
src/bin/pg_basebackup/streamutil.c: 63 - 281

## Overview
Establishes a connection to PostgreSQL server using provided connection parameters, with support for replication connections and automatic password handling.

## Definition


## Detailed Description
The GetConnection function creates a PostgreSQL database connection with specialized handling for replication connections used by pg_basebackup utilities. It merges connection parameters from connection strings and individual options, handles password prompts when needed, and performs security validations including setting a secure search path and verifying integer_datetimes compatibility. The function automatically retries connection attempts when password authentication is required and performs essential security checks before returning a valid connection.

## Parameters / Member Variables
- No parameters (uses global variables: , , , , , , , )

## Dependencies
- Functions called/Symbols referenced:
  - PQconninfoParse
  - PQconnectdbParams  
  - PQstatus
  - PQconnectionNeedsPassword
  - PQfinish
  - PQexec
  - PQparameterStatus
  - PQserverVersion
  - RetrieveDataDirCreatePerm
  - simple_prompt
  - pg_malloc0
- Called from (representative examples):
  - main (in pg_basebackup.c, pg_receivewal.c, pg_recvlogical.c)
  - StartLogStreamer
  - StreamLog
  - StreamLogicalLog
  - setup_connection (in pg_dump.c)

## Notes and Other Information
- Returns NULL on non-permanent errors, calls exit(1) on permanent errors
- Automatically sets dbname to "replication" for replication connections
- Sets secure search path for PostgreSQL 10+ servers when using database connections
- Validates integer_datetimes compatibility between client and server
- Retrieves and configures data directory permissions via RetrieveDataDirCreatePerm
- Supports both connection string format and individual parameter specification