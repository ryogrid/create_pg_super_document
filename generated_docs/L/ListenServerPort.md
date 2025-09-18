# ListenServerPort

## Location
src/backend/libpq/pqcomm.c: 417 - 683

## Overview
Creates and configures listening sockets for PostgreSQL server connections, supporting both TCP/IP and Unix domain socket communication with proper address binding and connection queue setup.

## Definition


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
  - pg_getaddrinfo_all
  - pg_freeaddrinfo_all
  - pg_getnameinfo_all
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