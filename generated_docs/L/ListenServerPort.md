# ListenServerPort

## Location
[src/backend/libpq/pqcomm.c:417-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L417-L683)

## Overview
Creates and configures listening sockets for PostgreSQL server connections, supporting both TCP/IP and Unix domain socket communication with proper address binding and connection queue setup.

## Definition

```c
struct addrinfo *addrs = NULL,
			   *addr;
```
## Detailed Description
The  function is responsible for creating and configuring listening sockets that the PostgreSQL postmaster uses to accept client connections. It supports multiple address families (IPv4, IPv6, and Unix domain sockets) and can create multiple listening sockets simultaneously.

Key operations performed:
- Address resolution using  for the specified hostname and port
- Socket creation with appropriate address family (AF_INET, AF_INET6, AF_UNIX)
- Socket option configuration (SO_REUSEADDR, IPV6_V6ONLY, FD_CLOEXEC)
- Unix domain socket path creation and locking for exclusive access
- Address binding to the resolved addresses
- Listen queue setup with capacity based on MaxConnections
- Error handling and logging for each step of the process

The function iterates through all resolved addresses and attempts to bind to each one, accumulating successfully created sockets in the provided array. For Unix sockets, it handles path creation and file system permissions setup.

## Parameters / Member Variables
- : Address family (AF_UNIX, AF_UNSPEC for TCP, etc.)
- : Host interface to bind to (NULL for all interfaces for TCP)
- : Port number to listen on
- : Directory for Unix domain socket (required for AF_UNIX, ignored for TCP)
- : Output array to store successfully created socket file descriptors
- : Input/output parameter for number of sockets in the array
- : Maximum size of the ListenSockets array

## Dependencies
- Functions called/Symbols referenced:
  - [pg_getaddrinfo_all](../p/pg_getaddrinfo_all.md)
  - [pg_freeaddrinfo_all](../p/pg_freeaddrinfo_all.md)
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md)
  - socket
  - bind
  - listen
  - setsockopt
  - [Lock_AF_UNIX](Lock_AF_UNIX.md)
  - [Setup_AF_UNIX](../S/Setup_AF_UNIX.md)
  - UNIXSOCK_PATH
  - closesocket
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)

## Notes and Other Information
- Returns STATUS_OK if at least one socket was successfully created, STATUS_ERROR otherwise
- Supports both IPv4 and IPv6 with proper dual-stack handling via IPV6_V6ONLY
- Unix domain sockets require exclusive locking to prevent multiple postmaster instances
- Listen queue size is set to MaxConnections * 2 for optimal connection handling
- Platform-specific socket options are conditionally applied (Windows vs Unix)
- Comprehensive error reporting includes hints about potential postmaster conflicts

## Simplified Source

```c
// Simplified version of ListenServerPort
int ListenServerPort(int family, const char *hostName, unsigned short portNumber,
                     const char *unixSocketDir,
                     pgsocket ListenSockets[], int *NumListenSockets, int MaxListen)
{
    pgsocket fd;
    struct addrinfo *addrs = NULL, *addr;
    struct addrinfo hint;
    int added = 0;
    char unixSocketPath[MAXPGPATH];
    char portNumberStr[32];
    const char *service;
    int one = 1;

    // Initialize address resolution hints
    MemSet(&hint, 0, sizeof(hint));
    hint.ai_family = family;
    hint.ai_flags = AI_PASSIVE;
    hint.ai_socktype = SOCK_STREAM;

    // Set up service string based on socket type
    if (family == AF_UNIX) {
        // Create Unix socket path and lock it
        UNIXSOCK_PATH(unixSocketPath, portNumber, unixSocketDir);
        if (strlen(unixSocketPath) >= UNIXSOCK_PATH_BUFLEN) {
            ereport(LOG, (errmsg("Unix socket path too long")));
            return STATUS_ERROR;
        }
        if (Lock_AF_UNIX(unixSocketDir, unixSocketPath) != STATUS_OK)
            return STATUS_ERROR;
        service = unixSocketPath;
    } else {
        // Use port number for TCP/IP
        snprintf(portNumberStr, sizeof(portNumberStr), "%d", portNumber);
        service = portNumberStr;
    }

    // Resolve addresses for the service
    if (pg_getaddrinfo_all(hostName, service, &hint, &addrs) || !addrs) {
        ereport(LOG, (errmsg("could not resolve address for service")));
        return STATUS_ERROR;
    }

    // Try to create socket for each resolved address
    for (addr = addrs; addr; addr = addr->ai_next) {
        // Skip if we've reached the maximum number of sockets
        if (*NumListenSockets == MaxListen) {
            ereport(LOG, (errmsg("maximum listen sockets exceeded")));
            break;
        }

        // Create socket
        fd = socket(addr->ai_family, SOCK_STREAM, 0);
        if (fd == PGINVALID_SOCKET) {
            // Log error and try next address
            continue;
        }

        // Set socket options for reusability and platform-specific settings
        if (addr->ai_family != AF_UNIX) {
            setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        }

        // Set IPv6-only flag if needed
        if (addr->ai_family == AF_INET6) {
            setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof(one));
        }

        // Bind socket to address
        if (bind(fd, addr->ai_addr, addr->ai_addrlen) < 0) {
            ereport(LOG, (errmsg("could not bind to address")));
            closesocket(fd);
            continue;
        }

        // Set up Unix socket permissions if needed
        if (addr->ai_family == AF_UNIX) {
            if (Setup_AF_UNIX(service) != STATUS_OK) {
                closesocket(fd);
                break;
            }
        }

        // Start listening with appropriate queue size
        int maxconn = MaxConnections * 2;
        if (listen(fd, maxconn) < 0) {
            ereport(LOG, (errmsg("could not listen on socket")));
            closesocket(fd);
            continue;
        }

        // Successfully created socket - add to array
        ereport(LOG, (errmsg("listening on socket")));
        ListenSockets[*NumListenSockets] = fd;
        (*NumListenSockets)++;
        added++;
    }

    // Clean up address info
    pg_freeaddrinfo_all(hint.ai_family, addrs);

    // Return success if at least one socket was created
    return added ? STATUS_OK : STATUS_ERROR;
}
```

Key simplifications made:
- Removed detailed error messages and logging specifics for clarity
- Consolidated address family handling into simpler conditional blocks
- Abstracted platform-specific socket option details
- Simplified error handling to focus on main execution flow
- Reduced verbose logging to essential status messages
- Consolidated similar error cases into generic handlers
- Focused on the core algorithm: resolve addresses, create sockets, bind, listen