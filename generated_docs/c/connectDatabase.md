# connectDatabase

## Location
[src/bin/pg_dump/pg_dumpall.c:1757-1945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1757-L1945)

## Overview
Establishes a PostgreSQL database connection with comprehensive parameter handling, password prompting, version compatibility checking, and connection string management.

## Definition


## Detailed Description
This function provides a robust connection establishment mechanism for PostgreSQL client utilities, particularly pg_dumpall. It handles complex connection parameter merging from multiple sources (connection strings, individual parameters), implements interactive password prompting with retry logic, and performs comprehensive connection validation.

The function merges connection parameters from a connection string with individual parameters (host, port, user, etc.), explicitly filtering out any dbname from the connection string to avoid conflicts. It supports automatic password prompting when needed, maintains password state across retry attempts, and validates server version compatibility to ensure proper operation.

After establishing the connection, it constructs and stores a canonical connection string for later use, performs server version validation against supported ranges (9.2+ to current major version), and executes security-related initialization queries before returning the connection.

## Parameters / Member Variables
- : Target database name to connect to
- : Optional connection string with additional parameters
- : PostgreSQL server hostname or address
- : PostgreSQL server port number
- : Username for authentication
- : Tristate value controlling password prompting behavior (TRI_YES/TRI_NO/TRI_DEFAULT)
- : If true, function exits on connection failure; if false, returns NULL

## Dependencies
- Functions called/Symbols referenced:
  - [trivalue](../t/trivalue.md) (enum type for tristate values)
  - [PQconninfoOption](../P/PQconninfoOption.md) (PostgreSQL connection info structure)
  - [TRI_YES](../T/TRI_YES.md)/TRI_NO (tristate constants)
  - simple_prompt (password prompting utility)
  - [PQconninfoFree](../P/PQconninfoFree.md) (connection info cleanup)
  - [PQconninfoParse](../P/PQconninfoParse.md) (connection string parsing)
  - pg_malloc0 (memory allocation)
  - [PQconnectdbParams](../P/PQconnectdbParams.md) (PostgreSQL connection establishment)
  - PQstatus/CONNECTION_BAD (connection status checking)
  - [PQconnectionNeedsPassword](../P/PQconnectionNeedsPassword.md) (password requirement checking)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - [constructConnStr](constructConnStr.md) (connection string construction)
  - PQparameterStatus/PQserverVersion (version checking)
  - [executeQuery](../e/executeQuery.md)/ALWAYS_SECURE_SEARCH_PATH_SQL (security initialization)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dumpall.c at multiple lines)
  - [connectMaintenanceDatabase](connectMaintenanceDatabase.md) (in connect_utils.c)
  - Various database utilities (pg_amcheck, clusterdb, reindexdb, vacuumdb)

## Notes and Other Information
- Sets global 'connstr' variable with the successful connection string
- Maintains static password storage for retry attempts across calls
- Supports server versions from 9.2 up to the current major version
- Implements robust error handling with optional graceful failure mode
- Executes security initialization query (ALWAYS_SECURE_SEARCH_PATH_SQL) after connection
- Memory management handled carefully with proper cleanup on all exit paths
- Used widely across PostgreSQL client utilities for consistent connection handling
- Password prompting preserves entered passwords for subsequent connection attempts