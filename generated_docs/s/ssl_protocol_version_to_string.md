# ssl_protocol_version_to_string

## Location
[src/backend/libpq/be-secure-openssl.c:1726-1746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1726-L1746)

## Overview
Converts PostgreSQL TLS protocol version enumeration values to human-readable string representations for logging and display purposes.

## Definition
```c
static const char *ssl_protocol_version_to_string(int v)
```

## Detailed Description
This static function provides a simple mapping from PostgreSQL's internal TLS protocol version enumeration values to their corresponding human-readable string representations. It serves as a companion to ssl_protocol_version_to_openssl() and is primarily used for logging, error messages, and status reporting where administrators need to see which TLS protocol version is being used in a clear, standardized format.

The function handles all supported TLS protocol versions and provides a fallback for unrecognized values, ensuring robust operation even with invalid input.

## Parameters / Member Variables
- `v`: PostgreSQL internal TLS protocol version enumeration value to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - PG_TLS_ANY (PostgreSQL constant for any TLS version)
  - PG_TLS1_VERSION (PostgreSQL constant for TLS 1.0)
  - PG_TLS1_1_VERSION (PostgreSQL constant for TLS 1.1)
  - PG_TLS1_2_VERSION (PostgreSQL constant for TLS 1.2)
  - [PG_TLS1_3_VERSION](../P/PG_TLS1_3_VERSION.md) (PostgreSQL constant for TLS 1.3)
- Called from (representative examples):
  - [be_tls_open_server](../b/be_tls_open_server.md) (TLS server connection establishment with logging)

## Notes and Other Information
- Static function - only accessible within the be-secure-openssl.c compilation unit
- Returns string constants, so the returned pointer should not be freed by the caller
- Provides graceful handling of invalid input by returning "(unrecognized)"
- Used primarily for logging and debugging TLS connection establishment
- [String](../S/String.md) representations follow standard TLS version naming conventions
- Part of the comprehensive TLS configuration and status reporting system
- Complements ssl_protocol_version_to_openssl() for complete protocol version handling
- Essential for administrators to understand which TLS protocols are being negotiated and used