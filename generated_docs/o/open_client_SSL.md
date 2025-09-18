# open_client_SSL

## Location
src/interfaces/libpq/fe-secure-openssl.c: 1480 - 1643

## Overview
Attempts to negotiate and establish an SSL/TLS connection with a PostgreSQL server, handling the handshake process and performing certificate validation.

## Definition
```c
static PostgresPollingStatusType open_client_SSL(PGconn *conn)
```

## Detailed Description
This function performs the SSL/TLS handshake with the PostgreSQL server using OpenSSL's SSL_connect() function. It handles various SSL connection states and error conditions that can occur during the handshake process, providing appropriate error messages and recovery actions.

The function supports non-blocking operation by returning polling status indicators when the connection needs more data or when the socket is ready for writing. It performs comprehensive error handling for different SSL error types including syscall errors, SSL protocol errors, and certificate validation failures.

Key validation steps include:
- ALPN (Application Layer Protocol Negotiation) verification for direct SSL connections
- Server certificate retrieval and validation
- Peer name matching against the certificate
- Specific handling of certificate authority errors when using system CA pools

## Parameters / Member Variables
- `conn`: PGconn connection object with an initialized SSL context and configuration

## Dependencies
- Functions called/Symbols referenced:
  - SSL_connect (OpenSSL handshake function)
  - SSL_get_error / ERR_get_error (OpenSSL error handling)
  - SSL_get_verify_result (certificate verification status)
  - SSL_get0_alpn_selected (ALPN protocol verification)
  - SSL_get_peer_certificate (server certificate retrieval)
  - SSLerrmessage / SSLerrfree (libpq SSL error utilities)
  - pgtls_close (cleanup function)
  - pq_verify_peer_name_matches_certificate (hostname verification)
  - SOCK_ERRNO_SET / SOCK_ERRNO / SOCK_STRERROR (socket error handling)
- Called from:
  - pgtls_open_client

## Notes and Other Information
- Returns PostgresPollingStatusType values:
  - PGRES_POLLING_READING: needs to read more data from socket
  - PGRES_POLLING_WRITING: needs to write data to socket  
  - PGRES_POLLING_FAILED: connection failed with error
  - PGRES_POLLING_OK: SSL handshake completed successfully
- Handles non-blocking socket operations for asynchronous connection establishment
- Provides detailed error messages for SSL protocol version mismatches
- Special handling for X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY when using system CA pool
- Enforces ALPN protocol negotiation for direct SSL connections (ENC_SSL mode)
- Validates that ALPN negotiated the expected PostgreSQL protocol (PG_ALPN_PROTOCOL)
- Cleans up connection state via pgtls_close() on any failure condition
- Certificate verification logic is separate from the handshake (handled in initialize_SSL)
- Distinguishes between socket-level errors and SSL protocol errors for better diagnostics