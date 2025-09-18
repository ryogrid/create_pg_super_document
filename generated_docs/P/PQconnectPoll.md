# PQconnectPoll

## Location
src/interfaces/libpq/fe-connect.c: 2596 - 2873

## Overview
`PQconnectPoll` is a public libpq function that performs non-blocking polling of an asynchronous PostgreSQL database connection, advancing the connection state machine without blocking program execution.

## Definition
```c
PostgresPollingStatusType PQconnectPoll(PGconn *conn)
```

## Detailed Description
This function implements the core non-blocking connection establishment mechanism for PostgreSQL. It advances the connection through various states including hostname resolution, socket connection, SSL/GSS negotiation, authentication, and final connection establishment. The function handles multiple hosts/addresses with load balancing support (including random shuffling), connection timeouts, and automatic failover between servers. It manages the connection state machine through various states like CONNECTION_STARTED, CONNECTION_MADE, CONNECTION_AUTH_OK, etc., and handles both reading and writing states appropriately. The function supports Unix domain sockets, IPv4/IPv6 addresses, and hostname resolution, with special handling for prefer-standby connection modes.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure representing the database connection being polled

## Dependencies
- Functions called/Symbols referenced:
  - [pqReadData](../p/pqReadData.md): Reads incoming data from the connection
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md): Appends error messages to connection
  - [release_conn_addrinfo](../r/release_conn_addrinfo.md): Releases address information for previous host
  - [pqParseIntParam](../p/pqParseIntParam.md): Parses integer parameters like port numbers
  - `pg_getaddrinfo_all`: Resolves hostnames to network addresses
  - [store_conn_addrinfo](../s/store_conn_addrinfo.md): Stores address information in connection structure
  - `pg_freeaddrinfo_all`: Frees address information
  - `pg_prng_uint64_range`: Generates random numbers for load balancing
  - [pqDropConnection](../p/pqDropConnection.md): Closes existing connection
  - [pqDropServerData](../p/pqDropServerData.md): Clears server-specific data
  - [pqClearAsyncResult](../p/pqClearAsyncResult.md): Clears asynchronous result data
  - Connection state constants: CONNECTION_BAD, CONNECTION_OK, CONNECTION_NEEDED, etc.
  - Polling status constants: PGRES_POLLING_OK, PGRES_POLLING_READING, PGRES_POLLING_WRITING, PGRES_POLLING_FAILED

- Called from (representative examples):
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md): Blocking connection completion
  - [PQresetPoll](PQresetPoll.md): Connection reset polling
  - [pqConnectDBStart](../p/pqConnectDBStart.md): Connection startup
  - [PQcancelPoll](PQcancelPoll.md): Connection cancellation polling
  - [libpqrcv_connect](../l/libpqrcv_connect.md): Replication connection establishment
  - [wait_until_connected](../w/wait_until_connected.md): psql connection waiting
  - [libpqsrv_connect_internal](../l/libpqsrv_connect_internal.md): Server-side connection helper

## Notes and Other Information
This is a public API function that applications can call directly to implement non-blocking connection establishment. It should be used in conjunction with `PQconnectStart()` and requires the application to use `select()` or similar mechanisms to determine when to call it based on socket readiness. The function returns different PostgresPollingStatusType values: PGRES_POLLING_OK (connection complete), PGRES_POLLING_READING (waiting for socket readable), PGRES_POLLING_WRITING (waiting for socket writable), or PGRES_POLLING_FAILED (connection failed). Applications must call `PQfinish()` regardless of success or failure. The function includes important caveats about blocking behavior with hostname resolution, Kerberos authentication, and tracing operations.