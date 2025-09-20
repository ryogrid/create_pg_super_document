# PQescapeStringConn

## Location
[src/interfaces/libpq/fe-exec.c:4177-4198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4177-L4198)

## Overview
PQescapeStringConn is a public libpq function that escapes arbitrary strings for use in SQL queries, using connection-specific encoding and standard string settings.

## Definition

```c
size_t
PQescapeStringConn(PGconn *conn,
				   char *to, const char *from, size_t length,
				   int *error)
```
## Detailed Description
PQescapeStringConn provides a safe way to escape strings for inclusion in SQL queries by taking into account the specific characteristics of the database connection. The function uses the connection's client encoding and standard_conforming_strings setting to properly escape the input string.

This function is the preferred way to escape strings when a database connection is available, as it can make encoding-aware decisions and provide connection-specific error reporting. If the connection is NULL, the function returns an empty string and sets an error flag.

The function clears any existing connection error state before processing if there are no pending commands, ensuring clean error reporting for the escaping operation.

## Parameters / Member Variables
- : PostgreSQL connection handle (required - function fails if NULL)
- : Output buffer where the escaped string will be written (must be at least 2*length + 1 bytes)
- : Input string to be escaped
- : Maximum length of the source string to process
- : Pointer to int that will be set to 1 if errors occur (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - [PQescapeStringInternal](PQescapeStringInternal.md)
- Called from (representative examples):
  - [AppendStringCommandOption](../A/AppendStringCommandOption.md)
  - [check_loadable_libraries](../c/check_loadable_libraries.md)
  - [do_lo_import](../d/do_lo_import.md)
  - [escape_string](../e/escape_string.md)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md)
  - [escape_string_conn](../e/escape_string_conn.md)

## Notes and Other Information
- This is the recommended function for string escaping when a database connection is available
- The function automatically uses the connection's client_encoding and std_strings settings
- Returns 0 and sets error flag if conn is NULL
- Clears connection error state before processing when no commands are queued
- The output buffer must be at least 2*length + 1 bytes to accommodate worst-case escaping
- Always produces a NUL-terminated output string
- Provides connection-specific error reporting through the connection's error message facilities