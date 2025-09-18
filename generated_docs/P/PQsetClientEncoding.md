# PQsetClientEncoding

## Location
src/interfaces/libpq/fe-connect.c: 7255 - 7296

## Overview
Sets the client character encoding for the database connection, allowing applications to specify which character encoding to use for client-server communication.

## Definition


## Detailed Description
PQsetClientEncoding sets the client character encoding for the given database connection by executing a "SET client_encoding" SQL command on the backend. The function validates the connection state and encoding parameter, handles the special "auto" encoding value by resolving it from the system locale, and sends the appropriate SQL command to the server. The actual encoding change is reported back by the backend server, and the client state is updated accordingly through the parameter status reporting mechanism.

## Parameters / Member Variables
- : The database connection handle (must be in CONNECTION_OK state)
- : The name of the character encoding to set (e.g., "UTF8", "LATIN1", or "auto" for locale-based detection)

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_OK (connection status constant)
  - pg_encoding_to_char (converts encoding ID to string)
  - pg_get_encoding_from_locale (gets encoding from system locale)
  - PQexec (executes SQL command)
  - PGRES_COMMAND_OK (result status constant)
  - PQclear (frees result object)
- Called from (representative examples):
  - setup_connection (in pg_dump.c)
  - main (in pg_dumpall.c)
  - exec_command_encoding (in psql command.c)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- The "auto" encoding value is automatically resolved to the appropriate encoding based on the system locale
- Includes buffer overflow protection when constructing the SQL query
- The function relies on the backend to report the actual parameter change through the parameter status mechanism
- Connection must be in CONNECTION_OK state for the function to succeed