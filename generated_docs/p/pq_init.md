# pq_init

## Location
src/backend/libpq/pqcomm.c: 173 - 332

## Overview
Initializes libpq communication infrastructure at backend startup, setting up network socket options, communication buffers, and wait event handling for client-server communication.

## Definition


## Detailed Description
The  function is responsible for initializing the PostgreSQL backend's communication layer with a client. It takes a  from the postmaster and creates a fully configured  structure that will be used for all subsequent client communication during the backend process lifetime.

Key operations performed:
- Copies client socket information to a new  structure
- Retrieves and stores local (server) socket address information
- Configures TCP-specific socket options (NODELAY, KEEPALIVE) for network connections
- On Windows, optimizes OS send buffer size for performance
- Applies keepalive parameters for connection monitoring
- Initializes communication buffers and state variables
- Sets up the socket for non-blocking operation (on Unix systems)
- Creates wait event set for asynchronous I/O operations
- Registers cleanup handler for process exit

## Parameters / Member Variables
- : Pointer to ClientSocket structure containing the client connection socket and address information from the postmaster

## Dependencies
- Functions called/Symbols referenced:
  - palloc0
  - memcpy
  - getsockname
  - setsockopt
  - pq_setkeepalivesidle
  - pq_setkeepalivesinterval
  - pq_setkeepalivescount
  - pq_settcpusertimeout
  - MemoryContextAlloc
  - on_proc_exit
  - socket_close
  - pg_set_noblock
  - CreateWaitEventSet
  - AddWaitEventToSet
- Called from (representative examples):
  - BackendInitialize

## Notes and Other Information
- This function is called once per backend process during initialization
- Socket configuration differs between TCP and Unix domain socket connections
- Windows-specific optimizations are applied for send buffer sizing
- The function sets up non-blocking I/O with wait events for interruptible communication
- Process exit cleanup is automatically registered to close the socket
- The returned Port structure becomes the primary interface for all backend-client communication