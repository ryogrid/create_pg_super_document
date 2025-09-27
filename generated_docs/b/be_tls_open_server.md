# be_tls_open_server

## Location
[src/backend/libpq/be-secure-openssl.c:435-730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L435-L730)

## Overview
Establishes an SSL/TLS connection with a client by performing the SSL handshake, configuring ALPN protocol negotiation, and extracting client certificate information.

## Definition

```c
int
be_tls_open_server(Port *port)
```
## Detailed Description
The  function performs the SSL/TLS server-side handshake with a connecting client. It creates an SSL connection object, associates it with the client socket, performs the handshake negotiation, and extracts client certificate information for authentication purposes.

Key operations include:
1. **SSL Connection Setup**: Creates an SSL object from the global SSL context and associates it with the client socket
2. **Callback Configuration**: Sets up info and ALPN (Application Layer Protocol Negotiation) callbacks
3. **SSL Handshake**: Performs the SSL_accept() handshake with proper error handling and retry logic for non-blocking operations
4. **ALPN Processing**: Checks for and validates ALPN protocol negotiation results
5. **Client Certificate Extraction**: Retrieves and processes the client certificate, extracting Common Name (CN) and Distinguished Name (DN) for authentication
6. **Security Validation**: Performs security checks including embedded null detection in certificate fields

The function handles various SSL error conditions with appropriate error reporting and includes retry logic for non-blocking socket operations.

## Parameters / Member Variables
- : Pointer to the Port structure representing the client connection. Must have port->ssl and port->peer initially set to NULL. The function populates various SSL-related fields in this structure upon success.

## Dependencies
- Functions called/Symbols referenced:
  - SSL_CTX_set_info_callback (debugging callback setup)
  - SSL_CTX_set_alpn_select_cb (ALPN protocol negotiation)
  - SSL_new (SSL connection object creation)
  - [my_SSL_set_fd](../m/my_SSL_set_fd.md) (socket association)
  - SSL_accept (SSL handshake)
  - SSL_get_error (error code retrieval)
  - ERR_get_error / ERR_clear_error (OpenSSL error handling)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md) (non-blocking I/O waiting)
  - SSL_get0_alpn_selected (ALPN result retrieval)
  - SSL_get_peer_certificate (client certificate retrieval)
  - X509_get_subject_name / X509_NAME_get_text_by_NID (certificate parsing)
  - X509_NAME_print_ex (DN formatting)
  - BIO_new / BIO_get_mem_ptr (certificate data extraction)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (memory allocation)
  - [SSLerrmessage](../S/SSLerrmessage.md) (error message formatting)
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md) (socket error codes)
  - [ssl_protocol_version_to_string](../s/ssl_protocol_version_to_string.md) (protocol version formatting)

- Called from (representative examples):
  - [secure_open_server](../s/secure_open_server.md) (in be-secure.c:132)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- The function asserts that port->ssl and port->peer are initially NULL
- Requires the global SSL_context to be initialized (via be_tls_init)
- Handles both blocking and non-blocking socket operations with appropriate retry logic
- Implements comprehensive error handling for various SSL failure scenarios
- Provides detailed protocol version hints for SSL protocol mismatch errors
- Validates client certificates by checking for embedded null characters (security against CVE-2009-4034)
- Supports ALPN protocol negotiation with validation against expected PostgreSQL protocol
- Extracts certificate information in RFC2253 format for DN representation
- Uses PostgreSQL's memory context system for certificate data allocation
- The port->ssl_in_use flag is set to true upon successful SSL object creation
- Error reporting uses COMMERROR level for communication errors
- Properly manages OpenSSL per-thread error queues to ensure reliable SSL_get_error() operation

## Simplified Source

```c
// Simplified version of be_tls_open_server
int be_tls_open_server(Port *port) {
    int r, err;
    unsigned long ecode;

    Assert(!port->ssl && !port->peer);

    // Check SSL context is initialized
    if (!SSL_context) {
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
               errmsg("could not initialize SSL connection: SSL context not set up")));
        return -1;
    }

    // Set up SSL callbacks
    SSL_CTX_set_info_callback(SSL_context, info_cb);
    SSL_CTX_set_alpn_select_cb(SSL_context, alpn_cb, port);

    // Create SSL connection object
    if (!(port->ssl = SSL_new(SSL_context))) {
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
               errmsg("could not initialize SSL connection: %s",
                      SSLerrmessage(ERR_get_error()))));
        return -1;
    }

    // Associate SSL with socket
    if (!my_SSL_set_fd(port, port->sock)) {
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
               errmsg("could not set SSL socket: %s",
                      SSLerrmessage(ERR_get_error()))));
        return -1;
    }

    port->ssl_in_use = true;

    // Perform SSL handshake with retry logic
retry_accept:
    errno = 0;
    ERR_clear_error();
    r = SSL_accept(port->ssl);

    if (r <= 0) {
        err = SSL_get_error(port->ssl, r);
        ecode = ERR_get_error();

        switch (err) {
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                // Handle non-blocking I/O - wait and retry
                WaitLatchOrSocket(MyLatch,
                    err == SSL_ERROR_WANT_READ ? WL_SOCKET_READABLE : WL_SOCKET_WRITEABLE,
                    port->sock, 0, WAIT_EVENT_SSL_OPEN_SERVER);
                goto retry_accept;

            case SSL_ERROR_SYSCALL:
                // System call error
                ereport(COMMERROR, (errcode_for_socket_access(),
                       errmsg("could not accept SSL connection: %m")));
                return -1;

            case SSL_ERROR_SSL:
                // SSL protocol error with optional version hint
                ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("could not accept SSL connection: %s",
                              SSLerrmessage(ecode))));
                return -1;

            default:
                ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("unrecognized SSL error code: %d", err)));
                return -1;
        }
    }

    // Process ALPN protocol negotiation
    const unsigned char *selected;
    unsigned int len;
    SSL_get0_alpn_selected(port->ssl, &selected, &len);

    port->alpn_used = false;
    if (selected != NULL) {
        if (len == strlen(PG_ALPN_PROTOCOL) &&
            memcmp(selected, PG_ALPN_PROTOCOL, strlen(PG_ALPN_PROTOCOL)) == 0) {
            port->alpn_used = true;
        } else {
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                   errmsg("received SSL connection request with unexpected ALPN protocol")));
        }
    }

    // Extract client certificate information
    port->peer = SSL_get_peer_certificate(port->ssl);
    port->peer_cn = NULL;
    port->peer_dn = NULL;
    port->peer_cert_valid = false;

    if (port->peer != NULL) {
        // Extract Common Name and Distinguished Name
        X509_NAME *x509name = X509_get_subject_name(port->peer);

        // Get Common Name with security validation
        int len = X509_NAME_get_text_by_NID(x509name, NID_commonName, NULL, 0);
        if (len != -1) {
            char *peer_cn = MemoryContextAlloc(TopMemoryContext, len + 1);
            X509_NAME_get_text_by_NID(x509name, NID_commonName, peer_cn, len + 1);
            peer_cn[len] = '\0';

            // Security check for embedded nulls
            if (len == strlen(peer_cn)) {
                port->peer_cn = peer_cn;
            } else {
                ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("SSL certificate's common name contains embedded null")));
                pfree(peer_cn);
                return -1;
            }
        }

        // Extract Distinguished Name in RFC2253 format
        BIO *bio = BIO_new(BIO_s_mem());
        if (bio) {
            BUF_MEM *bio_buf;
            if (X509_NAME_print_ex(bio, x509name, 0, XN_FLAG_RFC2253) != -1 &&
                BIO_get_mem_ptr(bio, &bio_buf) > 0) {

                char *peer_dn = MemoryContextAlloc(TopMemoryContext, bio_buf->length + 1);
                memcpy(peer_dn, bio_buf->data, bio_buf->length);
                peer_dn[bio_buf->length] = '\0';

                // Security check for embedded nulls
                if (bio_buf->length == strlen(peer_dn)) {
                    port->peer_dn = peer_dn;
                    port->peer_cert_valid = true;
                } else {
                    ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                           errmsg("SSL certificate's distinguished name contains embedded null")));
                    pfree(peer_dn);
                    BIO_free(bio);
                    return -1;
                }
            }
            BIO_free(bio);
        }
    }

    return 0;
}
```

Key simplifications made:
- Condensed SSL handshake retry logic while maintaining essential flow
- Simplified error handling cases while preserving core error types
- Abstracted detailed protocol version checking in SSL_ERROR_SSL case
- Streamlined certificate processing while maintaining security checks
- Focused on main execution path while preserving critical error handling
- Maintained essential OpenSSL API calls and security validations