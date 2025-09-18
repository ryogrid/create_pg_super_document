# ssl_protocol_version_to_openssl

## Location
src/backend/libpq/be-secure-openssl.c: 1691 - 1725

## Overview
Converts PostgreSQL TLS protocol version GUC enumeration values to their corresponding OpenSSL constant values.

## Definition
```c
static int ssl_protocol_version_to_openssl(int v)
```

## Detailed Description
This static function provides a mapping layer between PostgreSQL's internal TLS protocol version enumeration (used in GUC parameters like ssl_min_protocol_version and ssl_max_protocol_version) and OpenSSL's protocol version constants. The function ensures that PostgreSQL's TLS configuration is independent of OpenSSL availability and version while still being able to configure the underlying OpenSSL library properly.

The function includes conditional compilation directives to handle different OpenSSL versions that may not support certain TLS protocol versions, returning -1 for unsupported versions and allowing calling code to detect and handle unsupported protocols gracefully.

## Parameters / Member Variables
- `v`: PostgreSQL internal TLS protocol version enumeration value to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_TLS_ANY (PostgreSQL constant for any TLS version)
  - PG_TLS1_VERSION (PostgreSQL constant for TLS 1.0)
  - PG_TLS1_1_VERSION (PostgreSQL constant for TLS 1.1)
  - PG_TLS1_2_VERSION (PostgreSQL constant for TLS 1.2)
  - PG_TLS1_3_VERSION (PostgreSQL constant for TLS 1.3)
  - TLS1_VERSION (OpenSSL constant for TLS 1.0)
  - TLS1_1_VERSION (OpenSSL constant for TLS 1.1, if available)
  - TLS1_2_VERSION (OpenSSL constant for TLS 1.2, if available)
  - TLS1_3_VERSION (OpenSSL constant for TLS 1.3, if available)
- Called from (representative examples):
  - be_tls_init (backend TLS initialization)
  - initialize_SSL (libpq SSL initialization)

## Notes and Other Information
- Static function - only accessible within the be-secure-openssl.c compilation unit
- Returns 0 for PG_TLS_ANY to indicate no specific version restriction
- Returns -1 for unsupported or unrecognized protocol versions
- Uses conditional compilation to handle different OpenSSL versions gracefully
- Mirrors similar functionality in libpq's fe-secure-openssl.c for consistency
- Essential for configuring OpenSSL context with user-specified minimum and maximum TLS versions
- Allows PostgreSQL to maintain protocol version independence from specific OpenSSL installations
- Part of the GUC (Grand Unified Configuration) system integration for TLS settings