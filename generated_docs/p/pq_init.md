# pq_init

## Location
[src/backend/libpq/pqcomm.c:173-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L173-L332)

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
  - [palloc0](palloc0.md)
  - memcpy
  - getsockname
  - setsockopt
  - [pq_setkeepalivesidle](pq_setkeepalivesidle.md)
  - [pq_setkeepalivesinterval](pq_setkeepalivesinterval.md)
  - [pq_setkeepalivescount](pq_setkeepalivescount.md)
  - [pq_settcpusertimeout](pq_settcpusertimeout.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [on_proc_exit](../o/on_proc_exit.md)
  - [socket_close](../s/socket_close.md)
  - [pg_set_noblock](pg_set_noblock.md)
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)
- Called from (representative examples):
  - [BackendInitialize](../B/BackendInitialize.md)

## Notes and Other Information
- This function is called once per backend process during initialization
- Socket configuration differs between TCP and Unix domain socket connections
- Windows-specific optimizations are applied for send buffer sizing
- The function sets up non-blocking I/O with wait events for interruptible communication
- Process exit cleanup is automatically registered to close the socket
- The returned Port structure becomes the primary interface for all backend-client communication