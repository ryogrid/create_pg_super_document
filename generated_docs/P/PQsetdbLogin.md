# PQsetdbLogin

## Location
[src/interfaces/libpq/fe-connect.c:1919-2033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L1919-L2033)

## Overview
Establishes a synchronous connection to a PostgreSQL backend through the postmaster at the specified host and port with login credentials.

## Definition

```c
structure.  Note that we also expect this
	 * to initialize conn->errorMessage to empty.  All subsequent steps during
	 * connection initialization will only append to that buffer.
	 */
	conn = pqMakeEmptyPGconn();
```
## Detailed Description
This function creates a synchronous connection to a PostgreSQL database server using the traditional parameter-based interface. It allocates and initializes a PGconn structure, processes the provided connection parameters, validates and computes derived options, and establishes the actual connection. The function returns immediately after the connection attempt is complete (either successful or failed).

The function supports two modes of operation:
1. If dbName contains a connection string, it parses it as a full connection specification
2. Otherwise, it treats each parameter individually and builds the connection using traditional parameter assignment

Key operations performed:
- Allocates an empty PGconn structure using pqMakeEmptyPGconn()
- Detects and handles connection strings in the dbName parameter
- Sets up default connection options and overrides them with provided parameters
- Calls pqConnectOptions2() to validate and compute derived options
- Initiates the connection with pqConnectDBStart() and completes it with pqConnectDBComplete()

## Parameters / Member Variables
- : Database server hostname or IP address (NULL for default)
- : Port number as string (NULL for default)
- : Command-line options to be sent to the server (NULL for none)
- : Unused parameter kept for backward compatibility (ignored)
- : Database name to connect to, or a full connection string
- : Username for authentication (NULL for default)
- /home/ryo/work/postgres_17_6_sub: Password for authentication (NULL for none)

## Dependencies
- Functions called/Symbols referenced:
  - [pqMakeEmptyPGconn](../p/pqMakeEmptyPGconn.md)
  - [recognized_connection_string](../r/recognized_connection_string.md)
  - [connectOptions1](../c/connectOptions1.md)
  - [pqConnectOptions2](../p/pqConnectOptions2.md)
  - [pqConnectDBStart](../p/pqConnectDBStart.md)
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md)
- Called from (representative examples):
  - PQsetdb (convenience macro)

## Notes and Other Information
- Returns a PGconn pointer which is required for all subsequent libpq calls
- If connection fails, the returned PGconn will have status CONNECTION_BAD and errorMessage will contain details
- The pgtty parameter is maintained for backward compatibility but is no longer used
- Memory allocation failures are handled by returning a PGconn with CONNECTION_BAD status
- The function performs a complete synchronous connection attempt before returning
- Supports both traditional parameter-based connections and modern connection string formats
- All string parameters are internally duplicated, so caller-provided strings can be freed after the call