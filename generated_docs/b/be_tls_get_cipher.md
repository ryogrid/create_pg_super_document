# be_tls_get_cipher

## Location
[src/backend/libpq/be-secure-openssl.c:1507-1515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1507-L1515)

## Overview
Returns the name of the cipher suite being used for an active TLS connection.

## Definition
```c
const char *be_tls_get_cipher(Port *port)
```

## Detailed Description
This function retrieves the name of the cipher suite currently being used for a TLS connection. It wraps the OpenSSL function `SSL_get_cipher()` to provide this information to PostgreSQL's backend. The function returns a string representation of the cipher suite (e.g., "ECDHE-RSA-AES256-GCM-SHA384") if a TLS connection is active, or NULL if no TLS connection exists.

The cipher suite name provides detailed information about the cryptographic algorithms being used for the connection, including the key exchange method, authentication algorithm, symmetric encryption algorithm, and message authentication code. This information is crucial for security auditing and ensuring that strong cryptographic standards are being used.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing a client connection that may have an active TLS session

## Dependencies
- Functions called/Symbols referenced:
  - SSL_get_cipher (OpenSSL function)
  - [Port](../P/Port.md) (structure containing SSL connection state)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (for collecting connection statistics)
  - [PerformAuthentication](../P/PerformAuthentication.md) (during authentication logging)

## Notes and Other Information
- Returns NULL if no SSL connection is active on the port
- The returned string is managed by OpenSSL and should not be freed by the caller
- Cipher suite names follow standard naming conventions (e.g., "AES256-GCM-SHA384")
- This function is part of PostgreSQL's TLS abstraction layer for OpenSSL
- Used for security monitoring, compliance checking, and debugging
- The cipher suite determines the cryptographic strength and performance characteristics of the connection
- Located in src/backend/libpq/be-secure-openssl.c:1507-1515