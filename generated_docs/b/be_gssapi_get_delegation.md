# be_gssapi_get_delegation

## Location
[src/backend/libpq/be-secure-gssapi.c:779-785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-gssapi.c#L779-L785)

## Overview
Returns whether GSSAPI delegated credentials were included during authentication on the given connection.

## Definition
```c
bool be_gssapi_get_delegation(Port *port)
```

## Detailed Description
This function checks if GSSAPI delegated credentials were provided during the authentication process for a specific PostgreSQL backend connection. Delegated credentials in GSSAPI allow the server to act on behalf of the authenticated client, enabling the server to access other services using the client's credentials.

The function performs safety checks to ensure the port and its GSSAPI structure are valid before accessing the delegation flag. If GSSAPI authentication was not performed or if the connection is invalid, the function returns false.

The delegation status is stored in the port's GSSAPI structure (port->gss->delegated_creds) as a boolean flag that indicates whether the client included delegated credentials during the GSSAPI authentication exchange.

## Parameters / Member Variables
- `port`: A pointer to the Port structure representing the client connection. Contains all connection state information including GSSAPI authentication details and delegation status.

## Dependencies
- Functions called/Symbols referenced:
  - [Port](../P/Port.md) (structure access)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (backend statistics initialization)
  - [PerformAuthentication](../P/PerformAuthentication.md) (authentication process - multiple calls)
  - Referenced in libpq-be.h (header declaration)

## Notes and Other Information
- Returns a boolean value indicating the presence of delegated credentials
- Performs null pointer checks for both port and port->gss to ensure safe access
- Delegated credentials enable the PostgreSQL server to authenticate to other services on behalf of the client
- This function is part of PostgreSQL's GSSAPI authentication infrastructure
- Used for authentication verification, logging, and determining available authentication capabilities
- The delegation status is set during the GSSAPI authentication handshake and remains constant for the connection lifetime