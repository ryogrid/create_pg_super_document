# PQconndefaults

## Location
[src/interfaces/libpq/fe-connect.c:1881-1918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L1881-L1918)

## Overview
Constructs a default connection options array that identifies all available connection options and shows default values from environment variables and system settings.

## Definition

```c
structure.  Note that we also expect this
	 * to initialize conn->errorMessage to empty.  All subsequent steps during
	 * connection initialization will only append to that buffer.
	 */
	conn = pqMakeEmptyPGconn();
```
## Detailed Description
This function creates and returns a dynamically allocated array of PQconninfoOption structures that contains all possible PostgreSQL connection parameters with their current default values. The defaults are determined from environment variables, system configuration, and built-in defaults. This function is useful for applications that need to discover all available connection options and their current default values before establishing a connection.

The function performs the following operations:
- Initializes an error buffer for internal operations
- Calls conninfo_init() to create the basic connection options structure
- Calls conninfo_add_defaults() to populate default values from environment and system settings
- Handles memory allocation failures gracefully
- Returns a dynamically allocated array that must be freed with PQconninfoFree()

The returned array is terminated by an entry with a NULL keyword field.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - PQExpBufferDataBroken
  - [conninfo_init](../c/conninfo_init.md)
  - [conninfo_add_defaults](../c/conninfo_add_defaults.md)
  - [PQconninfoFree](PQconninfoFree.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Called from (representative examples):
  - [GetDbnameFromConnectionOptions](../G/GetDbnameFromConnectionOptions.md) (pg_basebackup)
  - [check_pghost_envvar](../c/check_pghost_envvar.md) (pg_upgrade)
  - [do_connect](../d/do_connect.md) (psql)
  - [main](../m/main.md) (libpq_uri_regress test)

## Notes and Other Information
- Returns NULL on error (typically out of memory)
- The returned array is dynamically allocated and must be freed with PQconninfoFree()
- Prior to PostgreSQL 7.0, this function returned a static array, which was not thread-safe
- Applications using this function should always call PQconninfoFree() to avoid memory leaks
- The function doesn't report specific errors but handles them internally
- Each option in the returned array contains keyword, environment variable name, compiled default, current value, label, and display information

## Simplified Source

```c
PQconninfoOption *PQconndefaults(void)
{
    PQExpBufferData errorBuf;
    PQconninfoOption *connOptions;

    // Initialize error buffer for internal operations
    initPQExpBuffer(&errorBuf);
    if (PQExpBufferDataBroken(errorBuf)) {
        return NULL;  // Out of memory
    }

    // Create initial connection options structure
    connOptions = conninfo_init(&errorBuf);

    if (connOptions != NULL) {
        // Add default values from environment and system settings
        if (!conninfo_add_defaults(connOptions, NULL)) {
            // Failed to add defaults - clean up and return NULL
            PQconninfoFree(connOptions);
            connOptions = NULL;
        }
    }

    // Clean up error buffer
    termPQExpBuffer(&errorBuf);

    return connOptions;
}
```