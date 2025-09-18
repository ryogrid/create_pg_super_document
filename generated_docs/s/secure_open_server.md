# secure_open_server

## Location
src/backend/libpq/be-secure.c: 110 - 162

## Overview
Establishes a secure SSL/TLS session with a client by negotiating encryption and handling the transition from unencrypted to encrypted communication.

## Definition


## Detailed Description
The `secure_open_server` function manages the server-side SSL/TLS handshake process and the critical transition from unencrypted to encrypted communication. It handles a complex scenario where some data may have already been buffered before the SSL negotiation begins, requiring careful management of unencrypted data that needs to be processed through the SSL layer.

The function first preserves any unencrypted data that was already read from the client, then calls `be_tls_open_server` to perform the actual SSL handshake. After successful negotiation, it verifies that no unencrypted data remains (which would indicate a protocol violation), cleans up temporary buffers, and logs connection details including the client's Distinguished Name (DN) and Common Name (CN) from the SSL certificate.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing the client connection. Contains socket information, buffering state, and SSL-related fields including peer certificate details.

## Dependencies
- Functions called/Symbols referenced:
  - pq_buffer_remaining_data (checks for buffered unencrypted data)
  - pq_startmsgread/pq_endmsgread (message reading protocol functions)
  - pq_getbytes (reads buffered data)
  - be_tls_open_server (performs actual SSL handshake)
  - palloc/pfree (PostgreSQL memory management)
  - ereport (PostgreSQL logging system)
  - STATUS_ERROR (error return constant)
- Called from (representative examples):
  - ProcessSSLStartup (during SSL connection establishment)
  - ProcessStartupPacket (as part of connection startup sequence)
  - FeBeWaitSetNEvents (referenced in libpq.h)

## Dependencies
- Functions called/Symbols referenced:
  - pq_buffer_remaining_data
  - pq_startmsgread
  - pq_getbytes
  - pq_endmsgread
  - be_tls_open_server
  - palloc
  - pfree
  - ereport
  - STATUS_ERROR
  - DEBUG2
- Called from (representative examples):
  - ProcessSSLStartup
  - ProcessStartupPacket
  - FeBeWaitSetNEvents

## Notes and Other Information
- Returns 0 on success, STATUS_ERROR on failure
- Handles the complex transition from unencrypted to encrypted communication
- Logs SSL connection details at DEBUG2 level including client certificate information
- When SSL is not compiled in, the function becomes a no-op that always returns 0
- Critical for maintaining protocol integrity during SSL handshake
- The function includes assertions and error checking to detect protocol violations