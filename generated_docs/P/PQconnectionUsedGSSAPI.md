# PQconnectionUsedGSSAPI

## Location
src/interfaces/libpq/fe-connect.c: 7236 - 7246

## Overview
Indicates whether GSSAPI authentication was used during the establishment of a PostgreSQL connection.

## Definition


## Detailed Description
The PQconnectionUsedGSSAPI function determines whether GSSAPI (Generic Security Services Application Program Interface) authentication was used during the connection establishment process. GSSAPI is a standardized interface for security services that supports various authentication mechanisms including Kerberos. This function examines the connection's gssapi_used flag, which is set when GSSAPI authentication was successfully employed during the connection handshake.

This function is valuable for client applications that need to understand the authentication method used for security auditing, logging, or to make decisions based on the authentication mechanism that was employed.

## Parameters / Member Variables
- : Pointer to the PGconn connection object to query for GSSAPI usage information. If NULL, the function returns false.

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses conn->gssapi_used directly)
- Called from (representative examples):
  - Referenced in libpq-fe.h interface

## Notes and Other Information
- Returns int where non-zero (true) indicates GSSAPI authentication was used, zero (false) indicates it was not
- GSSAPI is commonly used in enterprise environments for single sign-on and integrated authentication
- This function only reports on successful connections where GSSAPI was part of the completed authentication process
- The function safely handles NULL connection pointers by returning false
- Part of the libpq client interface for connection introspection and security verification
- GSSAPI support in PostgreSQL enables integration with Kerberos and other enterprise authentication systems