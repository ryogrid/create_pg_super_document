# pgtls_open_client

## Location
[src/interfaces/libpq/fe-secure-openssl.c:118-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L118-L139)

## Overview
Establishes or continues an SSL/TLS client connection handshake with a PostgreSQL server, handling both initial SSL object creation and ongoing handshake progression.

## Definition


## Detailed Description
This function manages the SSL/TLS client-side connection establishment process. It operates in two phases: first-time initialization where it creates the SSL object and loads certificates, and subsequent calls where it continues the SSL handshake process. The function is designed to work with PostgreSQL's non-blocking connection model, returning polling status to indicate whether the connection is complete, needs more data, or has failed.

On first invocation (when conn->ssl is NULL), it calls initialize_SSL() to set up the SSL object with client certificates, private keys, and trusted CA certificates. On subsequent calls, it delegates to open_client_SSL() to continue the handshake process.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object (PGconn) containing SSL state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - [initialize_SSL](../i/initialize_SSL.md)
  - [pgtls_close](pgtls_close.md)  
  - PGRES_POLLING_FAILED
  - [open_client_SSL](../o/open_client_SSL.md)
- Called from (representative examples):
  - [pqsecure_open_client](pqsecure_open_client.md) (in fe-secure.c:156)
  - pgunlock_thread (referenced in libpq-int.h:805)

## Notes and Other Information
- Returns PostgresPollingStatusType to indicate connection status (PGRES_POLLING_OK, PGRES_POLLING_READING, PGRES_POLLING_WRITING, or PGRES_POLLING_FAILED)
- Part of the non-blocking connection establishment mechanism in libpq
- Automatically cleans up on initialization failure by calling pgtls_close()
- The function is stateful and can be called multiple times for the same connection to progress through the handshake
- Error messages from initialization failures are stored in conn->errorMessage by initialize_SSL()
- Location: src/interfaces/libpq/fe-secure-openssl.c:118-137