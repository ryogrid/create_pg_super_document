# PQsslAttribute

## Location
[src/interfaces/libpq/fe-secure-openssl.c:1841-1907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L1841-L1907)

## Overview
Returns the value of a specified SSL attribute for a PostgreSQL connection, providing detailed information about SSL connection properties.

## Definition

```c
const char *
PQsslAttribute(PGconn *conn, const char *attribute_name)
```
## Detailed Description
PQsslAttribute is a public API function in PostgreSQL's libpq library that retrieves specific SSL attribute values from an active PostgreSQL connection. The function supports querying various SSL properties including cryptographic details, protocol information, and negotiated features.

The function handles different connection states:
- When conn is NULL and attribute_name is "library": Returns "OpenSSL" (the default SSL library)
- When conn->ssl is NULL (no SSL): Returns NULL for all attributes
- When SSL is active: Returns specific attribute values based on OpenSSL function calls

Supported attributes include:
- "library": Always returns "OpenSSL"
- "key_bits": Number of bits in the encryption key
- "cipher": Name of the SSL cipher being used
- "compression": "on" or "off" indicating SSL compression status
- "protocol": SSL/TLS protocol version (e.g., "TLSv1.2")
- "alpn": Application Layer Protocol Negotiation value

## Parameters / Member Variables
- : Pointer to the PostgreSQL connection object (PGconn structure), or NULL to query default library
- : String specifying which SSL attribute to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function for string comparison)
  - SSL_get_cipher_bits (OpenSSL function to get encryption key length)
  - snprintf (standard C library function for formatted string output)
  - SSL_get_cipher (OpenSSL function to get cipher name)
  - SSL_get_current_compression (OpenSSL function to check compression status)
  - SSL_get_version (OpenSSL function to get protocol version)
  - SSL_get0_alpn_selected (OpenSSL function to get ALPN negotiation result)
  - memcpy (standard C library function for memory copying)
  - conn->ssl (SSL object in PGconn structure)
- Called from (representative examples):
  - [printSSLInfo](../p/printSSLInfo.md) (in psql command implementation)
  - [print_ssl_library](../p/print_ssl_library.md) (in libpq test client)
  - External applications using libpq

## Notes and Other Information
- Returns NULL for unrecognized attribute names
- Returns NULL for all attributes when connection has no SSL
- Some return values use static buffers (key_bits, alpn) so values may be overwritten on subsequent calls
- The "alpn" attribute is limited to 255 bytes maximum length
- The function is declared in the public libpq-fe.h header, making it part of the official libpq API
- Works in conjunction with PQsslAttributeNames to provide complete SSL introspection capabilities
- Located in src/interfaces/libpq/fe-secure-openssl.c:1841-1907