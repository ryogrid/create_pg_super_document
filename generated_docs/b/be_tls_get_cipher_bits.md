# be_tls_get_cipher_bits

## Location
[src/backend/libpq/be-secure-openssl.c:1484-1497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1484-L1497)

## Overview
Returns the effective key length in bits for the cipher being used in a TLS connection.

## Definition
```c
int be_tls_get_cipher_bits(Port *port)
```

## Detailed Description
This function retrieves the effective key length (in bits) of the cipher suite currently being used for a TLS connection. It wraps the OpenSSL function `SSL_get_cipher_bits()` to provide this information to PostgreSQL's backend. The function checks if a valid SSL connection exists on the given port and returns the cipher strength, or 0 if no TLS connection is active.

The effective key length represents the actual cryptographic strength of the cipher being used, which may be different from the key size. This information is useful for security auditing, logging, and determining the strength of the encryption being used for client connections.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing a client connection that may have an active TLS session

## Dependencies
- Functions called/Symbols referenced:
  - SSL_get_cipher_bits (OpenSSL function)
  - [Port](../P/Port.md) (structure containing SSL connection state)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (for collecting connection statistics)
  - [PerformAuthentication](../P/PerformAuthentication.md) (during authentication logging)

## Notes and Other Information
- Returns 0 if no SSL connection is active on the port
- The returned value represents the effective key length, not necessarily the nominal key size
- This function is part of PostgreSQL's TLS abstraction layer for OpenSSL
- Used primarily for monitoring, logging, and security assessment purposes
- Located in src/backend/libpq/be-secure-openssl.c:1484-1497