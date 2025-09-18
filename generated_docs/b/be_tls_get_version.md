# be_tls_get_version

## Location
[src/backend/libpq/be-secure-openssl.c:1498-1506](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1498-L1506)

## Overview
Returns the TLS/SSL protocol version string for an active connection.

## Definition
```c
const char *be_tls_get_version(Port *port)
```

## Detailed Description
This function retrieves the TLS/SSL protocol version being used for a client connection. It wraps the OpenSSL function `SSL_get_version()` to provide this information to PostgreSQL's backend. The function returns a string representation of the protocol version (e.g., "TLSv1.2", "TLSv1.3") if a TLS connection is active, or NULL if no TLS connection exists.

This information is valuable for security monitoring, compliance checking, and debugging connection issues. Different TLS versions provide different security guarantees, and knowing which version is in use helps administrators ensure their connections meet security requirements.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing a client connection that may have an active TLS session

## Dependencies
- Functions called/Symbols referenced:
  - SSL_get_version (OpenSSL function)
  - [Port](../P/Port.md) (structure containing SSL connection state)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (for collecting connection statistics)
  - [PerformAuthentication](../P/PerformAuthentication.md) (during authentication logging)

## Notes and Other Information
- Returns NULL if no SSL connection is active on the port
- The returned string is managed by OpenSSL and should not be freed by the caller
- Common return values include "TLSv1.2", "TLSv1.3", etc.
- This function is part of PostgreSQL's TLS abstraction layer for OpenSSL
- Used for security auditing and connection monitoring
- Located in src/backend/libpq/be-secure-openssl.c:1498-1506