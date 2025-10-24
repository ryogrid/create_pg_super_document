# cert_cb

## Location
[src/interfaces/libpq/fe-secure-openssl.c:468-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L468-L491)

## Overview
Certificate selection callback function that tracks client certificate requests and responses during SSL handshake in libpq.

## Definition
```c
static int cert_cb(SSL *ssl, void *arg)
```

## Detailed Description
This function serves as a callback for OpenSSL during the SSL handshake process when the server sends a CertificateRequest message. Rather than dynamically selecting certificates, this callback is primarily used for tracking and logging purposes. It records whether the server has requested a client certificate and whether the client has a certificate available to send.

The callback operates on the principle that PostgreSQL libpq only supports sending a single pre-configured certificate via the sslcert parameter. Therefore, it doesn't perform actual certificate selection logic but rather maintains state information about certificate exchange for diagnostic and connection status purposes.

## Parameters / Member Variables
- `ssl`: Pointer to the SSL connection structure containing the current SSL session state
- `arg`: Void pointer that contains the PGconn structure cast as void*, providing access to the connection context

## Dependencies
- Functions called/Symbols referenced:
  - SSL_get_certificate (checks if a certificate is loaded in the SSL context)
- Called from (representative examples):
  - [initialize_SSL](../i/initialize_SSL.md) (registered as SSL certificate callback during SSL context setup)

## Notes and Other Information
- Always returns 1 to indicate successful callback execution to OpenSSL
- Sets conn->ssl_cert_requested flag to true whenever called, indicating server requested client authentication
- Sets conn->ssl_cert_sent flag to true only if a certificate is actually loaded and available for transmission
- Does not modify the SSL context or perform actual certificate selection
- Part of PostgreSQL's client certificate authentication tracking mechanism
- The callback is registered during SSL initialization and invoked automatically by OpenSSL during handshake

## Simplified Source

```c
static int cert_cb(SSL *ssl, void *arg) {
    PGconn *conn = arg;

    // Mark that server requested client certificate
    conn->ssl_cert_requested = true;

    // Check if we have a certificate to send
    if (SSL_get_certificate(ssl))
        conn->ssl_cert_sent = true;

    // Signal successful callback completion
    return 1;
}
```