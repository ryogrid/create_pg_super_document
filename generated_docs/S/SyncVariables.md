# SyncVariables

## Location
src/bin/psql/command.c: 4040 - 4082

## Overview
Synchronizes psql's internal variables with the current database connection state, ensuring consistency between the client configuration and the established connection parameters.

## Definition
```c
void SyncVariables(void)
```

## Detailed Description
This function is called whenever a new database connection is established to update psql's internal state variables to match the connection properties. It performs a bidirectional synchronization: first retrieving connection information from the PostgreSQL server and updating internal variables, then sending client-side settings back to the server.

The function updates both the global pset structure and psql's variable system with current connection details including database name, user, host, port, encoding, and server version information. It also ensures that client-side error reporting preferences are applied to the connection.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - PQclientEncoding()
  - PQserverVersion()
  - setFmtEncoding()
  - PQdb()
  - SetVariable() (multiple calls)
  - PQuser()
  - PQhost()
  - PQport()
  - pg_encoding_to_char()
  - PQparameterStatus()
  - formatPGVersionNumber()
  - PQsetErrorVerbosity()
  - PQsetErrorContextVisibility()
- Called from:
  - do_connect (at src/bin/psql/command.c:3794)
  - CheckConnection (at src/bin/psql/common.c:383)
  - Various locations in startup.c

## Notes and Other Information
- Updates the following psql variables: DBNAME, USER, HOST, PORT, ENCODING, SERVER_VERSION_NAME, SERVER_VERSION_NUM
- Synchronizes both pset.encoding and pset.popt.topt.encoding with the connection's client encoding
- Attempts to get the full server version string first via PQparameterStatus(), falling back to formatting the numeric version if unavailable
- Applies client-side error verbosity and context visibility settings to the connection
- Critical for maintaining consistency when switching between different database connections in psql