# ClientSocket

## Location
src/include/libpq/libpq-be.h: 239 - 243

## Overview
ClientSocket is a lightweight structure that holds an accepted socket connection and remote endpoint information, used for passing connection data from the postmaster to backend processes.

## Definition
```c
typedef struct ClientSocket
{
    pgsocket    sock;           /* File descriptor */
    SockAddr    raddr;          /* remote addr (client) */
} ClientSocket;
```

## Detailed Description
ClientSocket serves as a minimal container for essential connection information that needs to be transferred from the postmaster process to newly spawned backend processes. It represents the fundamental connection state at the point where a client connection has been accepted but before the full Port structure is initialized with additional connection details.

This structure is designed for efficiency in process communication, containing only the most critical pieces of information needed to establish the backend's connection handling: the socket file descriptor and the client's network address. The backend process uses this information to initialize its more comprehensive Port structure.

## Parameters / Member Variables
- `sock`: The socket file descriptor for the accepted client connection
- `raddr`: The remote socket address structure containing the client's network address and port information

## Dependencies
- Functions called/Symbols referenced:
  - pgsocket (PostgreSQL socket type definition)
  - [SockAddr](../S/SockAddr.md) (Socket address structure)
- Called from (representative examples):
  - [AcceptConnection](../A/AcceptConnection.md) (in src/backend/libpq/pqcomm.c:793)
  - [BackendStartup](../B/BackendStartup.md) (in src/backend/postmaster/postmaster.c:3545)
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (in src/backend/postmaster/launch_backend.c:233)
  - [BackendInitialize](../B/BackendInitialize.md) (in src/backend/tcop/backend_startup.c:122)

## Notes and Other Information
- Used primarily in the postmaster-to-backend process communication pathway
- Represents the minimal connection state needed for process handoff
- Serves as input for initializing the more comprehensive Port structure in backend processes
- Part of the backend parameter passing mechanism during process creation
- Essential component in PostgreSQL's multi-process architecture for client connection handling