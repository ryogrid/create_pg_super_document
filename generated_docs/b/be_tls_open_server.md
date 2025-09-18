# be_tls_open_server

## Location
[src/backend/libpq/be-secure-openssl.c:435-730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L435-L730)

## Overview
Establishes an SSL/TLS connection with a client by performing the SSL handshake, configuring ALPN protocol negotiation, and extracting client certificate information.

## Definition


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