# libpqrcv_get_conninfo

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 371 - 419

## Overview
Returns a user-displayable connection string from an active WAL receiver connection with security-sensitive information obfuscated.

## Definition
```c
static char *libpqrcv_get_conninfo(WalReceiverConn *conn)
```

## Detailed Description
The `libpqrcv_get_conninfo` function extracts connection information from an established WAL receiver connection and formats it into a human-readable string suitable for display in logs, error messages, or administrative interfaces. The function ensures security by automatically obfuscating sensitive information such as passwords.

The function retrieves the actual connection parameters that were used to establish the connection (which may differ from the original connection string due to defaults, environment variables, or connection processing). It then constructs a clean, space-separated key=value formatted string while filtering out debug options, empty values, and properly masking security-sensitive fields.

The obfuscation is based on libpq's internal metadata about which connection parameters should be hidden from display, ensuring consistent security practices with other PostgreSQL tools.

## Parameters / Member Variables
- `conn`: Active WAL receiver connection from which to extract connection information (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [PQconninfo](../P/PQconninfo.md) (retrieve connection options from active connection)
  - `initPQExpBuffer` (initialize string buffer for output)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (append formatted strings to buffer)
  - [PQconninfoFree](../P/PQconninfoFree.md) (free libpq-allocated connection info)
  - `PQExpBufferDataBroken` (check buffer state)
  - `termPQExpBuffer` (clean up string buffer)
  - [pstrdup](../p/pstrdup.md) (duplicate final string using PostgreSQL memory management)
  - `strchr` (check display characteristics of connection options)

- Called from (representative examples):
  - Registered in `PQWalReceiverFunctions` table as `walrcv_get_conninfo`
  - Used for logging connection details in WAL receiver processes
  - Called when displaying subscription connection information

## Notes and Other Information
- Returns a palloc'ed string that should be freed by the caller
- Automatically obfuscates passwords and other security-sensitive fields marked with '*' in dispchar
- Filters out debug options (marked with 'D' in dispchar) and empty values
- Uses libpq's `PQconninfo` to get actual connection parameters used, not original string
- Output format is space-separated key=value pairs compatible with PostgreSQL connection strings
- Returns NULL if memory allocation fails during string buffer operations
- Function requires an active connection (conn->streamConn must not be NULL)
- Provides safe way to display connection details without exposing credentials
- Useful for debugging replication connection issues while maintaining security