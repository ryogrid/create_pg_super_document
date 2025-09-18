# PQprotocolVersion

## Location
src/interfaces/libpq/fe-connect.c: 7139 - 7148

## Overview
Returns the protocol version number of the connection to the PostgreSQL server, indicating the version of the frontend/backend communication protocol being used.

## Definition


## Detailed Description
This function extracts and returns the major protocol version number from an active PostgreSQL connection. The protocol version determines the format and capabilities of communication between the client (frontend) and the PostgreSQL server (backend). It returns 0 if the connection is invalid or in a bad state, making it useful for validating connection status and protocol compatibility.

The function uses the PG_PROTOCOL_MAJOR macro to extract the major version number from the connection's pversion field, which contains the full protocol version information.

## Parameters / Member Variables
- : A pointer to the PGconn structure representing the database connection. Must not be NULL for valid results.

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_BAD (connection status constant)
  - PG_PROTOCOL_MAJOR (macro to extract major protocol version)
- Called from (representative examples):
  - handleCopyIn (in psql copy operations)
  - PQsetdb (in connection establishment)

## Notes and Other Information
- Returns 0 for invalid connections (NULL pointer or CONNECTION_BAD status)
- The protocol version is established during connection setup and remains constant for the lifetime of the connection
- Higher protocol versions typically support more advanced features and optimizations
- This function is part of the libpq public API and is commonly used by applications to check protocol compatibility before using version-specific features