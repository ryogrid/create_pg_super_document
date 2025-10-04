# ident_inet

## Location
[src/backend/libpq/auth.c:1678-1862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L1678-L1862)

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
- `*port`: Pointer to hbaPort structure containing connection information including remote/local addresses, HBA configuration, and user details
## Dependencies
- Functions called/Symbols referenced:
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md) (PostgreSQL network utilities)
  - [pg_getaddrinfo_all](../p/pg_getaddrinfo_all.md) (PostgreSQL network utilities)  
  - [pg_freeaddrinfo_all](../p/pg_freeaddrinfo_all.md) (PostgreSQL network utilities)
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

## Simplified Source

```c
// Simplified version of ident_inet
static int ident_inet(hbaPort *port) {
    char ident_user[IDENT_USERNAME_MAX + 1];
    pgsocket sock_fd = PGINVALID_SOCKET;
    char remote_addr_s[NI_MAXHOST], remote_port[NI_MAXSERV];
    char local_addr_s[NI_MAXHOST], local_port[NI_MAXSERV];
    char ident_query[80], ident_response[80 + IDENT_USERNAME_MAX];
    struct addrinfo *ident_serv = NULL, *local_addr = NULL;
    bool success = false;

    // Step 1: Convert addresses to string format
    pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
                       remote_addr_s, sizeof(remote_addr_s),
                       remote_port, sizeof(remote_port),
                       NI_NUMERICHOST | NI_NUMERICSERV);
    pg_getnameinfo_all(&port->laddr.addr, port->laddr.salen,
                       local_addr_s, sizeof(local_addr_s),
                       local_port, sizeof(local_port),
                       NI_NUMERICHOST | NI_NUMERICSERV);

    // Step 2: Resolve Ident server address (port 113)
    struct addrinfo hints = {0};
    hints.ai_flags = AI_NUMERICHOST;
    hints.ai_family = port->raddr.addr.ss_family;
    hints.ai_socktype = SOCK_STREAM;

    char ident_port[NI_MAXSERV];
    snprintf(ident_port, sizeof(ident_port), "%d", IDENT_PORT);

    if (pg_getaddrinfo_all(remote_addr_s, ident_port, &hints, &ident_serv) != 0 ||
        pg_getaddrinfo_all(local_addr_s, NULL, &hints, &local_addr) != 0) {
        goto cleanup;
    }

    // Step 3: Create and configure socket
    sock_fd = socket(ident_serv->ai_family, ident_serv->ai_socktype, ident_serv->ai_protocol);
    if (sock_fd == PGINVALID_SOCKET) {
        ereport(LOG, (errcode_for_socket_access(),
                     errmsg("could not create socket for Ident connection: %m")));
        goto cleanup;
    }

    // Step 4: Bind to local address and connect to Ident server
    if (bind(sock_fd, local_addr->ai_addr, local_addr->ai_addrlen) != 0) {
        ereport(LOG, (errcode_for_socket_access(),
                     errmsg("could not bind to local address \"%s\": %m", local_addr_s)));
        goto cleanup;
    }

    if (connect(sock_fd, ident_serv->ai_addr, ident_serv->ai_addrlen) != 0) {
        ereport(LOG, (errcode_for_socket_access(),
                     errmsg("could not connect to Ident server at address \"%s\", port %s: %m",
                            remote_addr_s, ident_port)));
        goto cleanup;
    }

    // Step 5: Send Ident query
    snprintf(ident_query, sizeof(ident_query), "%s,%s\r\n", remote_port, local_port);

    int rc;
    do {
        CHECK_FOR_INTERRUPTS();
        rc = send(sock_fd, ident_query, strlen(ident_query), 0);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        ereport(LOG, (errcode_for_socket_access(),
                     errmsg("could not send query to Ident server: %m")));
        goto cleanup;
    }

    // Step 6: Receive and parse response
    do {
        CHECK_FOR_INTERRUPTS();
        rc = recv(sock_fd, ident_response, sizeof(ident_response) - 1, 0);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        ereport(LOG, (errcode_for_socket_access(),
                     errmsg("could not receive response from Ident server: %m")));
        goto cleanup;
    }

    ident_response[rc] = '\0';
    success = interpret_ident_response(ident_response, ident_user);

    if (!success) {
        ereport(LOG, (errmsg("invalidly formatted response from Ident server: \"%s\"",
                            ident_response)));
    }

cleanup:
    // Step 7: Cleanup resources
    if (sock_fd != PGINVALID_SOCKET) closesocket(sock_fd);
    if (ident_serv) pg_freeaddrinfo_all(port->raddr.addr.ss_family, ident_serv);
    if (local_addr) pg_freeaddrinfo_all(port->laddr.addr.ss_family, local_addr);

    // Step 8: Complete authentication if successful
    if (success) {
        set_authn_id(port, ident_user);
        return check_usermap(port->hba->usermap, port->user_name, ident_user, false);
    }

    return STATUS_ERROR;
}
```