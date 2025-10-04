# PQconninfo

## Location
[src/interfaces/libpq/fe-connect.c:6946-6989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6946-L6989)

## Overview
A public libpq API function that returns the connection options used for an active PostgreSQL connection, extracting the current values from the connection object.

## Definition

```c
*/
PQconninfoOption *
PQconninfo(PGconn *conn)
```
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
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - PQExpBufferDataBroken
  - [conninfo_init](../c/conninfo_init.md)
  - [conninfo_storeval](../c/conninfo_storeval.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Called from (representative examples):
  - [libpqrcv_get_conninfo](../l/libpqrcv_get_conninfo.md)
  - [do_connect](../d/do_connect.md) (psql)
  - [GenerateRecoveryConfig](../G/GenerateRecoveryConfig.md)
  - [copy_connection](../c/copy_connection.md) (test code)

## Notes and Other Information
- This is a public libpq API function exposed to client applications
- Returns NULL if the connection pointer is NULL or if memory allocation fails
- The returned array must be freed using PQconninfoFree() to prevent memory leaks
- Only extracts options that have valid connection object offsets (connofs >= 0)
- Silently ignores missing or invalid options during extraction (ignoreMissing=true)
- The function creates a complete snapshot of the current connection parameters
- Used by PostgreSQL tools like psql for connection introspection and by replication components
- Important for applications that need to clone or inspect existing connections

## Simplified Source

```c
// Simplified version of PQconninfo
PQconninfoOption *PQconninfo(PGconn *conn) {
    PQExpBufferData errorBuf;
    PQconninfoOption *connOptions;

    // Step 1: Validate connection
    if (conn == NULL) {
        return NULL;
    }

    // Step 2: Initialize error buffer (not used for errors, but required by callees)
    initPQExpBuffer(&errorBuf);
    if (PQExpBufferDataBroken(errorBuf)) {
        return NULL;  // Out of memory
    }

    // Step 3: Initialize connection options array
    connOptions = conninfo_init(&errorBuf);

    if (connOptions != NULL) {
        // Step 4: Extract all connection parameters from the connection object
        const internalPQconninfoOption *option;

        for (option = PQconninfoOptions; option->keyword; option++) {
            char **connmember;

            // Skip options that don't have connection object offsets
            if (option->connofs < 0) {
                continue;
            }

            // Get pointer to the connection member for this option
            connmember = (char **) ((char *) conn + option->connofs);

            // If the connection has a value for this option, store it
            if (*connmember) {
                conninfo_storeval(connOptions, option->keyword, *connmember,
                                 &errorBuf, true, false);
            }
        }
    }

    // Step 5: Clean up error buffer
    termPQExpBuffer(&errorBuf);

    return connOptions;
}
```