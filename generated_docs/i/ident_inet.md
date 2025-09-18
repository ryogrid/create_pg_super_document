# ident_inet

## Location
src/backend/libpq/auth.c: 1678 - 1862

## Overview
Implements the client-side Ident protocol communication to authenticate a PostgreSQL connection by querying the remote system's identification server.

## Definition
```c
static int ident_inet(hbaPort *port)
```

## Detailed Description
ident_inet performs RFC 1413 Ident protocol authentication by establishing a connection to the Ident server (typically running on port 113) on the client's machine and querying who owns the TCP connection. This authentication method relies on the trustworthiness of the client's Ident daemon.

The function implements the complete Ident protocol workflow:

1. **Address Resolution**: Converts the client and server socket addresses to string format for the Ident query
2. **Socket Setup**: Creates and configures a socket for communication with the remote Ident server
3. **Address Binding**: Binds to the local address that the client originally contacted, ensuring the Ident server can properly match the connection
4. **Ident Query**: Sends a query in the format "remote_port,local_port\r\n" to ask who owns the connection
5. **Response Processing**: Receives and parses the Ident server's response using interpret_ident_response
6. **Authentication Completion**: If successful, sets the authenticated identity and performs user mapping validation

The function includes comprehensive error handling for network operations and proper resource cleanup regardless of success or failure.

## Parameters / Member Variables
- : Pointer to hbaPort structure containing connection information including remote/local addresses, HBA configuration, and user details

## Dependencies
- Functions called/Symbols referenced:
  - pg_getnameinfo_all (PostgreSQL network utilities)
  - pg_getaddrinfo_all (PostgreSQL network utilities)  
  - pg_freeaddrinfo_all (PostgreSQL network utilities)
  - socket/bind/connect/send/recv (socket API)
  - closesocket (socket cleanup)
  - [interpret_ident_response](interpret_ident_response.md)
  - [set_authn_id](../s/set_authn_id.md) (PostgreSQL authentication)
  - [check_usermap](../c/check_usermap.md) (PostgreSQL authorization)
  - CHECK_FOR_INTERRUPTS (PostgreSQL interrupt handling)
  - ereport (PostgreSQL error reporting)
  - snprintf (C standard library)
  - strlen (C standard library)
  - IDENT_PORT
  - IDENT_USERNAME_MAX
  - Various socket/network constants and types
- Called from (representative examples):
  - IDENT_PORT context
  - HOSTNAME_LOOKUP_DETAIL context

## Notes and Other Information
- Implements RFC 1413 Identification Protocol for PostgreSQL authentication
- Requires the client system to be running an Ident daemon (typically identd) on port 113
- Uses the same local address that the client originally connected to, which is crucial for multi-homed servers or systems with IP aliases
- Includes interrupt handling during network operations to allow cancellation of slow Ident queries
- The authentication relies entirely on the trustworthiness of the remote Ident server - it can be easily spoofed
- Network operations include retry logic for EINTR (interrupted system calls)
- Performs proper resource cleanup with a centralized cleanup section using goto
- Returns STATUS_OK on successful authentication and authorization, STATUS_ERROR otherwise
- The function establishes its own TCP connection to the Ident server, separate from the PostgreSQL connection
- All network errors are logged at LOG level to help with debugging connection issues
- The Ident protocol is considered deprecated in modern environments due to security concerns
- Successfully authenticated users still must pass through the configured user mapping rules