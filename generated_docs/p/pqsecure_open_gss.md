# pqsecure_open_gss

## Location
[src/interfaces/libpq/fe-secure-gssapi.c:479-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-gssapi.c#L479-L755)

## Overview
Negotiates GSSAPI transport encryption for a PostgreSQL connection, managing the complete handshake process and buffer setup for secure communication.

## Definition


## Detailed Description
This function implements the complete GSSAPI transport negotiation process for PostgreSQL connections. It manages a complex state machine that handles multiple phases: initial buffer allocation, credential delegation setup, GSS context initialization, packet exchange with the server, and final buffer resizing for normal operation. The function operates in a non-blocking manner, returning polling status codes to indicate when the caller should retry based on socket readiness.

Key phases:
1. **Initialization**: Allocates authentication-sized buffers (PQ_GSS_AUTH_BUFFER_SIZE)
2. **Token Exchange**: Sends/receives GSSAPI tokens with the server using gss_init_sec_context
3. **Error Handling**: Processes server error packets during negotiation  
4. **Completion**: Resizes buffers to normal operation size (PQ_GSS_MAX_PACKET_SIZE) and determines maximum packet size

The function maintains state across multiple calls, handling partial reads/writes and resuming from the correct point in the negotiation process.

## Parameters / Member Variables
- : PostgreSQL connection object containing GSSAPI context, credentials, buffers, and socket information

## Dependencies
- Functions called/Symbols referenced:
  - gss_init_sec_context (primary GSSAPI negotiation function)
  - gss_wrap_size_limit (determines maximum packet size for encryption)
  - gss_release_buffer, gss_release_cred (GSSAPI resource cleanup)
  - [pg_GSS_load_servicename](pg_GSS_load_servicename.md) (loads Kerberos service principal)
  - [pg_GSS_have_cred_cache](pg_GSS_have_cred_cache.md) (acquires cached credentials for delegation)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting)
  - [gss_read](../g/gss_read.md) (internal wrapper for receiving data)
  - [pqsecure_raw_write](pqsecure_raw_write.md) (low-level socket write)
  - pg_hton32, pg_ntoh32 (network byte order conversion)
- Called from:
  - CONNECTION_FAILED state handler in fe-connect.c during connection establishment

## Notes and Other Information
- Uses different buffer sizes during negotiation (PQ_GSS_AUTH_BUFFER_SIZE) vs normal operation (PQ_GSS_MAX_PACKET_SIZE)
- Handles credential delegation when gssdelegation connection parameter is enabled
- Implements special error packet detection (packets starting with 'E') during startup phase
- Sets conn->gssenc and conn->gssapi_used flags upon successful completion
- Returns PGRES_POLLING_OK (success), PGRES_POLLING_READING/WRITING (retry needed), or PGRES_POLLING_FAILED (error)
- Critical for establishing secure GSSAPI transport before any application data exchange
- Manages global state variables for send/receive buffers that are later used by pg_GSS_read/write functions