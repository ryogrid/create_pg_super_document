# PQconninfo

## Location
src/interfaces/libpq/fe-connect.c: 6946 - 6989

## Overview
A public libpq API function that returns the connection options used for an active PostgreSQL connection, extracting the current values from the connection object.

## Definition


## Detailed Description
This function extracts the connection parameters from an active PostgreSQL connection and returns them as an array of PQconninfoOption structures. It provides a way for applications to introspect the actual connection settings being used, which may differ from the original connection string due to defaults, environment variables, or server-side configuration.

The function works by:
1. **Validation**: Checks if the connection pointer is valid
2. **Initialization**: Creates a new connOptions array using `conninfo_init`
3. **Extraction**: Iterates through all possible connection options and extracts their current values from the connection object
4. **Population**: Uses `conninfo_storeval` to populate the options array with the current connection values

This is particularly useful for debugging connection issues, logging actual connection parameters, or creating new connections with the same settings.

## Parameters / Member Variables
- `conn`: Pointer to an active PostgreSQL connection object (PGconn)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - PQExpBufferDataBroken
  - conninfo_init
  - conninfo_storeval
  - termPQExpBuffer
- Called from (representative examples):
  - libpqrcv_get_conninfo
  - do_connect (psql)
  - GenerateRecoveryConfig
  - copy_connection (test code)

## Notes and Other Information
- This is a public libpq API function exposed to client applications
- Returns NULL if the connection pointer is NULL or if memory allocation fails
- The returned array must be freed using PQconninfoFree() to prevent memory leaks
- Only extracts options that have valid connection object offsets (connofs >= 0)
- Silently ignores missing or invalid options during extraction (ignoreMissing=true)
- The function creates a complete snapshot of the current connection parameters
- Used by PostgreSQL tools like psql for connection introspection and by replication components
- Important for applications that need to clone or inspect existing connections