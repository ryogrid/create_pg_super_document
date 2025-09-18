# GetConnection

## Location
[src/bin/pg_basebackup/streamutil.c:63-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L63-L281)

## Overview
Establishes a connection to PostgreSQL server using provided connection parameters, with support for replication connections and automatic password handling.

## Definition


## Detailed Description
The GetConnection function creates a PostgreSQL database connection with specialized handling for replication connections used by pg_basebackup utilities. It merges connection parameters from connection strings and individual options, handles password prompts when needed, and performs security validations including setting a secure search path and verifying integer_datetimes compatibility. The function automatically retries connection attempts when password authentication is required and performs essential security checks before returning a valid connection.

## Parameters / Member Variables
- No parameters (uses global variables: , , , , , , , )

## Dependencies
- Functions called/Symbols referenced:
  - [PQconninfoParse](../P/PQconninfoParse.md)
  - [PQconnectdbParams](../P/PQconnectdbParams.md)  
  - PQstatus
  - [PQconnectionNeedsPassword](../P/PQconnectionNeedsPassword.md)
  - [PQfinish](../P/PQfinish.md)
  - [PQexec](../P/PQexec.md)
  - PQparameterStatus
  - [PQserverVersion](../P/PQserverVersion.md)
  - [RetrieveDataDirCreatePerm](../R/RetrieveDataDirCreatePerm.md)
  - simple_prompt
  - pg_malloc0
- Called from (representative examples):
  - [main](../m/main.md) (in pg_basebackup.c, pg_receivewal.c, pg_recvlogical.c)
  - [StartLogStreamer](../S/StartLogStreamer.md)
  - [StreamLog](../S/StreamLog.md)
  - [StreamLogicalLog](../S/StreamLogicalLog.md)
  - [setup_connection](../s/setup_connection.md) (in pg_dump.c)

## Notes and Other Information
- Returns NULL on non-permanent errors, calls exit(1) on permanent errors
- Automatically sets dbname to "replication" for replication connections
- Sets secure search path for PostgreSQL 10+ servers when using database connections
- Validates integer_datetimes compatibility between client and server
- Retrieves and configures data directory permissions via RetrieveDataDirCreatePerm
- Supports both connection string format and individual parameter specification