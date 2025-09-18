# PQgssEncInUse

## Location
src/interfaces/libpq/fe-secure.c: 504 - 518

## Overview
Determines whether GSSAPI encryption is currently active and usable for a PostgreSQL client connection.

## Definition
```c
int PQgssEncInUse(PGconn *conn)
```

## Detailed Description
This function checks if GSSAPI encryption is both enabled and actively being used for the specified PostgreSQL connection. GSSAPI encryption provides secure, encrypted communication between the client and server using the Generic Security Services API framework, typically with Kerberos as the underlying mechanism.

The function performs two checks: first, it verifies that a valid connection exists and that a GSSAPI security context has been established (conn->gctx is not NULL). Second, it checks the connection's internal gssenc flag, which indicates whether GSSAPI encryption has been successfully negotiated and is ready for use.

This function is useful for applications that need to verify the security status of their database connections, implement security policies, or provide user feedback about connection security.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle to check for GSSAPI encryption status

## Dependencies
- Functions called/Symbols referenced:
  - None (simple field access and logical operations)
- Called from (representative examples):
  - printGSSInfo (in psql command.c:4001 - for displaying connection security info)
- Connection fields accessed:
  - conn->gctx (GSSAPI security context)
  - conn->gssenc (GSSAPI encryption status flag)

## Notes and Other Information
- Returns 1 (true) if GSSAPI encryption is active and usable, 0 (false) otherwise
- Returns 0 if conn is NULL or if no GSSAPI context is established (conn->gctx is NULL)
- The gssenc flag is set to true during successful GSSAPI encryption negotiation in fe-secure-gssapi.c:673
- GSSAPI encryption is separate from GSSAPI authentication - a connection can have GSSAPI authentication without encryption
- This function should be called after connection establishment to get accurate results
- Only available when PostgreSQL is compiled with GSSAPI support (ENABLE_GSS)
- GSSAPI encryption mode is controlled by the 'gssencmode' connection parameter (disable, prefer, require)
- Used by psql to display connection security information in \conninfo command
- The function is thread-safe as it only performs read operations on connection fields