# AcceptConnection

## Location
src/backend/libpq/pqcomm.c: 793 - 828

## Overview
Accepts a new client connection on the server socket and populates the client socket structure with connection information.

## Definition


## Detailed Description
This function wraps the standard accept() system call to establish a new client connection. It accepts an incoming connection request on the server socket and fills in the client socket structure with the new connection's file descriptor and remote address information.

The function is designed to be used in a blocking context where the postmaster process has already determined that the server socket is ready to accept a new connection. If accept() fails, the function includes a brief delay (0.1 seconds) to prevent excessive CPU usage when the system is under resource pressure (such as when kernel file table slots are exhausted).

## Parameters / Member Variables
- : The server socket file descriptor that is ready to accept connections
- : Pointer to ClientSocket structure to be filled with the new connection's information (file descriptor and remote address)

## Dependencies
- Functions called/Symbols referenced:
  - accept (system call to accept incoming connections)
  - PGINVALID_SOCKET (PostgreSQL constant for invalid socket)
  - ereport/errmsg (PostgreSQL logging functions)
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md) (PostgreSQL error code function)
  - [pg_usleep](../p/pg_usleep.md) (PostgreSQL sleep function)
  - STATUS_OK (success return value)
  - STATUS_ERROR (error return value)
  - [ClientSocket](../C/ClientSocket.md) (structure type for client connection info)
  - pgsocket (PostgreSQL socket type)

- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster loop when accepting new connections)

## Notes and Other Information
- The function assumes it doesn't need to be non-blocking because the postmaster waits for the socket to be ready before calling accept()
- Includes a 100ms delay after accept() failure to prevent CPU spinning when resources are exhausted
- The most common cause of accept() failure is running out of kernel file table slots
- The client_sock->raddr structure is populated with the remote client's address information
- Returns STATUS_OK on successful connection acceptance, STATUS_ERROR on failure