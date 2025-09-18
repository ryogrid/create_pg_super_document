# PQconnectionUsedPassword

## Location
src/interfaces/libpq/fe-connect.c: 7225 - 7235

## Overview
Indicates whether password authentication was required and used during the establishment of a PostgreSQL connection.

## Definition


## Detailed Description
The PQconnectionUsedPassword function determines whether password authentication was required during the connection establishment process. Unlike PQconnectionNeedsPassword which checks if a password is currently needed, this function reports on the historical fact of whether password authentication was part of the successful connection process. It examines the connection's password_needed flag, which is set when the server requests password authentication during the connection handshake.

This function is useful for client applications that want to understand what authentication method was used for the connection, particularly for logging, security auditing, or informational purposes.

## Parameters / Member Variables
- : Pointer to the PGconn connection object to query for password usage information. If NULL, the function returns false.

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses conn->password_needed directly)
- Called from (representative examples):
  - [libpqrcv_connect](../l/libpqrcv_connect.md) (libpqwalreceiver.c)
  - [ConnectDatabase](../C/ConnectDatabase.md) (pg_backup_db.c)

## Notes and Other Information
- Returns int where non-zero (true) indicates password authentication was used, zero (false) indicates it was not
- This function only reports on successful connections - it indicates whether password auth was part of the completed authentication process
- Used primarily by replication components and backup utilities to verify authentication methods
- The function safely handles NULL connection pointers by returning false
- Part of the libpq client interface for connection introspection and security verification