# open_client_SSL

## Location
[src/interfaces/libpq/fe-secure-openssl.c:1480-1643](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L1480-L1643)

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
  - [SSLerrmessage](../S/SSLerrmessage.md) / SSLerrfree (libpq SSL error utilities)
  - [pgtls_close](../p/pgtls_close.md) (cleanup function)
  - [pq_verify_peer_name_matches_certificate](../p/pq_verify_peer_name_matches_certificate.md) (hostname verification)
  - SOCK_ERRNO_SET / SOCK_ERRNO / SOCK_STRERROR (socket error handling)
- Called from:
  - [pgtls_open_client](../p/pgtls_open_client.md)

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

## Simplified Source
```c
static PostgresPollingStatusType open_client_SSL(PGconn *conn) {
    int r;

    // Clear error state and attempt SSL handshake
    SOCK_ERRNO_SET(0);
    ERR_clear_error();
    r = SSL_connect(conn->ssl);

    if (r <= 0) {
        int save_errno = SOCK_ERRNO;
        int err = SSL_get_error(conn->ssl, r);
        unsigned long ecode = ERR_get_error();

        switch (err) {
            case SSL_ERROR_WANT_READ:
                return PGRES_POLLING_READING;   // Need more data

            case SSL_ERROR_WANT_WRITE:
                return PGRES_POLLING_WRITING;   // Socket ready for write

            case SSL_ERROR_SYSCALL:
                // Handle system call errors and certificate verification issues
                unsigned long vcode = SSL_get_verify_result(conn->ssl);

                if (save_errno == 0 &&
                    vcode == X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY &&
                    strcmp(conn->sslrootcert, "system") == 0) {
                    libpq_append_conn_error(conn, "SSL error: certificate verify failed: %s",
                                          X509_verify_cert_error_string(vcode));
                } else if (r == -1 && save_errno != 0) {
                    libpq_append_conn_error(conn, "SSL SYSCALL error: %s",
                                          SOCK_STRERROR(save_errno, sebuf, sizeof(sebuf)));
                } else {
                    libpq_append_conn_error(conn, "SSL SYSCALL error: EOF detected");
                }
                pgtls_close(conn);
                return PGRES_POLLING_FAILED;

            case SSL_ERROR_SSL:
                // Handle SSL protocol errors
                char *err_msg = SSLerrmessage(ecode);
                libpq_append_conn_error(conn, "SSL error: %s", err_msg);
                SSLerrfree(err_msg);

                // Provide additional guidance for protocol version errors
                switch (ERR_GET_REASON(ecode)) {
                    case SSL_R_NO_PROTOCOLS_AVAILABLE:
                    case SSL_R_UNSUPPORTED_PROTOCOL:
                    case SSL_R_BAD_PROTOCOL_VERSION_NUMBER:
                    case SSL_R_WRONG_VERSION_NUMBER:
                    case SSL_R_TLSV1_ALERT_PROTOCOL_VERSION:
                        libpq_append_conn_error(conn,
                            "This may indicate that the server does not support any SSL protocol version between %s and %s.",
                            conn->ssl_min_protocol_version ? conn->ssl_min_protocol_version : MIN_OPENSSL_TLS_VERSION,
                            conn->ssl_max_protocol_version ? conn->ssl_max_protocol_version : MAX_OPENSSL_TLS_VERSION);
                        break;
                    default:
                        break;
                }
                pgtls_close(conn);
                return PGRES_POLLING_FAILED;

            default:
                libpq_append_conn_error(conn, "unrecognized SSL error code: %d", err);
                pgtls_close(conn);
                return PGRES_POLLING_FAILED;
        }
    }

    // Verify ALPN for direct SSL connections
    if (conn->current_enc_method == ENC_SSL && conn->sslnegotiation[0] == 'd') {
        const unsigned char *selected;
        unsigned int len;

        SSL_get0_alpn_selected(conn->ssl, &selected, &len);

        if (selected == NULL) {
            libpq_append_conn_error(conn, "direct SSL connection was established without ALPN protocol negotiation extension");
            pgtls_close(conn);
            return PGRES_POLLING_FAILED;
        }

        if (len != strlen(PG_ALPN_PROTOCOL) ||
            memcmp(selected, PG_ALPN_PROTOCOL, strlen(PG_ALPN_PROTOCOL)) != 0) {
            libpq_append_conn_error(conn, "SSL connection was established with unexpected ALPN protocol");
            pgtls_close(conn);
            return PGRES_POLLING_FAILED;
        }
    }

    // Get and verify server certificate
    conn->peer = SSL_get_peer_certificate(conn->ssl);
    if (conn->peer == NULL) {
        char *err = SSLerrmessage(ERR_get_error());
        libpq_append_conn_error(conn, "certificate could not be obtained: %s", err);
        SSLerrfree(err);
        pgtls_close(conn);
        return PGRES_POLLING_FAILED;
    }

    // Verify hostname matches certificate
    if (!pq_verify_peer_name_matches_certificate(conn)) {
        pgtls_close(conn);
        return PGRES_POLLING_FAILED;
    }

    // SSL handshake completed successfully
    return PGRES_POLLING_OK;
}
```