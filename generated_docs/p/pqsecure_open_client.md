# pqsecure_open_client

## Location
[src/interfaces/libpq/fe-secure.c:153-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure.c#L153-L166)

## Overview
Initiates or continues the SSL/TLS handshake negotiation process for establishing a secure client connection.

## Definition
```c
PostgresPollingStatusType pqsecure_open_client(PGconn *conn)
```

## Detailed Description
pqsecure_open_client is an internal function that manages the SSL/TLS handshake process for PostgreSQL client connections. It serves as a wrapper that delegates to the underlying SSL implementation (pgtls_open_client) when SSL support is compiled in. The function supports both initial handshake attempts and continuation of partially completed handshakes, making it suitable for non-blocking connection establishment. When SSL support is not compiled in, it returns a failure status as secure connections should not be attempted in that case.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection structure representing the client connection

## Dependencies
- Functions called/Symbols referenced:
  - [pgtls_open_client](pgtls_open_client.md)
  - USE_SSL (conditional compilation flag)
  - PGRES_POLLING_FAILED
- Called from (representative examples):
  - CONNECTION_FAILED context in src/interfaces/libpq/fe-connect.c:3541
  - Referenced in src/interfaces/libpq/libpq-int.h:766

## Notes and Other Information
- Returns PostgresPollingStatusType indicating the status of the SSL handshake process
- Can return various polling statuses including OK, READING, WRITING, or FAILED
- Designed to support non-blocking I/O by allowing handshake to be resumed
- When USE_SSL is not defined, always returns PGRES_POLLING_FAILED with a comment indicating this should not happen
- This is an internal libpq function, not part of the public API
- Critical component in the secure connection establishment workflow
- Typically called during the connection state machine processing