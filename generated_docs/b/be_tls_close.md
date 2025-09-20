# be_tls_close

## Location
[src/backend/libpq/be-secure-openssl.c:731-760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L731-L760)

## Overview
Cleanly shuts down an SSL/TLS connection and frees all associated SSL resources and client certificate data for a given port.

## Definition

```c
void
be_tls_close(Port *port)
```
## Detailed Description
The  function performs a complete cleanup of SSL/TLS resources associated with a client connection. It properly shuts down the SSL connection, frees all SSL-related objects, and cleans up client certificate information that was extracted during the connection establishment.

The function performs the following cleanup operations in sequence:
1. **SSL Connection Shutdown**: If an SSL connection exists, performs SSL_shutdown() to cleanly terminate the SSL session
2. **SSL Object Cleanup**: Frees the SSL connection object and resets related flags
3. **Client Certificate Cleanup**: Frees the client certificate object if present
4. **Certificate Data Cleanup**: Frees the extracted Common Name and Distinguished Name strings

All operations include null-pointer checks to ensure safe cleanup even if some resources were never allocated or have already been freed.

## Parameters / Member Variables
- : Pointer to the Port structure representing the client connection. The function cleans up SSL-related fields including ssl, peer, peer_cn, peer_dn, and ssl_in_use.

## Dependencies
- Functions called/Symbols referenced:
  - SSL_shutdown (SSL connection termination)
  - SSL_free (SSL object cleanup)
  - X509_free (certificate object cleanup)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)

- Called from (representative examples):
  - [secure_close](../s/secure_close.md) (in be-secure.c:167)

## Notes and Other Information
- This function is idempotent and safe to call multiple times on the same port
- All cleanup operations are performed with null-pointer checks to prevent crashes
- The function does not return any status - it always succeeds in cleaning up available resources
- SSL_shutdown() is called to perform a proper SSL session termination, which sends a close_notify alert to the peer
- The ssl_in_use flag is set to false to indicate SSL is no longer active on this connection
- Memory allocated via PostgreSQL's memory context system (peer_cn, peer_dn) is freed using pfree()
- OpenSSL objects (SSL connection and X509 certificate) are freed using their respective OpenSSL cleanup functions
- This function should be called whenever a connection is being terminated to prevent memory leaks
- The function handles partial cleanup scenarios gracefully - if some resources were never allocated, they are simply skipped