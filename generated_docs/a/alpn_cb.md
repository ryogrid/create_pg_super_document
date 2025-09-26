# alpn_cb

## Location
[src/backend/libpq/be-secure-openssl.c:1323-1374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1323-L1374)

## Overview
A static callback function that handles Application-Layer Protocol Negotiation (ALPN) during SSL/TLS handshake to select the appropriate application protocol for PostgreSQL connections.

## Definition

```c
static int
alpn_cb(SSL *ssl,
		const unsigned char **out,
		unsigned char *outlen,
		const unsigned char *in,
		unsigned int inlen,
		void *userdata)
```
## Detailed Description
The  function implements the server-side ALPN callback for PostgreSQL's SSL/TLS connections. ALPN is a TLS extension (RFC 7301) that allows clients and servers to negotiate which application protocol to use over the secure connection. This callback is invoked during the SSL handshake when a client presents a list of supported protocols, and the server must select one from its own supported list.

The function uses OpenSSL's  helper function to find a mutually supported protocol from the client's offered protocols and the server's  list. If negotiation succeeds, the connection proceeds with the selected protocol. If no common protocol is found, the connection is rejected with a fatal TLS alert.

## Parameters / Member Variables
- : Pointer to the SSL connection structure for the current handshake
- : Output parameter that will point to the selected protocol identifier
- : Output parameter that will contain the length of the selected protocol identifier
- : Input buffer containing the client's list of supported protocols
- : Length of the client's protocol list buffer
- : User-defined data pointer (currently unused but validated)

## Dependencies
- Functions called/Symbols referenced:
  - SSL_select_next_proto (OpenSSL ALPN helper function)
  - alpn_protos (PostgreSQL's supported protocol list)
  - Assert (PostgreSQL assertion macro)
  - OPENSSL_NPN_NEGOTIATED (OpenSSL constant)
  - SSL_TLSEXT_ERR_OK (OpenSSL success return code)
  - SSL_TLSEXT_ERR_ALERT_FATAL (OpenSSL fatal error return code)
  - SSL_TLSEXT_ERR_NOACK (OpenSSL no acknowledgment return code)
- Called from (representative examples):
  - be_tls_open_server (registered as ALPN callback)

## Notes and Other Information
- This callback implements RFC 7301 (Application-Layer Protocol Negotiation for TLS)
- The function currently supports only PostgreSQL's specific protocol but uses the standard ALPN framework for future extensibility
- Failed negotiation results in a "no_application_protocol" TLS alert being sent to the client
- The callback includes validation of all input parameters using Assert macros
- Contains a comment noting the const/non-const mismatch in OpenSSL's helper function signature
- This functionality is only available when PostgreSQL is compiled with OpenSSL support and when ALPN is enabled