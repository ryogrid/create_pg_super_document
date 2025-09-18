# SSL_CTX_set_max_proto_version

## Location
src/common/protocol_openssl.c: 80 - 117

## Overview
Sets the maximum SSL/TLS protocol version for an SSL context by disabling newer protocol versions through SSL options.

## Definition
```c
int SSL_CTX_set_max_proto_version(SSL_CTX *ctx, int version)
```

## Detailed Description
This function is a compatibility implementation for older OpenSSL versions that lack the native SSL_CTX_set_max_proto_version function (introduced in OpenSSL 1.1.0). It works by setting SSL options to disable protocol versions newer than the specified maximum version. The function systematically disables TLS 1.1 and 1.2 if they are newer than the requested maximum version.

Unlike the minimum version function, this implementation does not disable SSLv2 and SSLv3 by default, as the maximum version setting is intended to cap the upper bound rather than enforce security minimums. The function includes an assertion to ensure the version parameter is not zero.

## Parameters / Member Variables
- `ctx`: The SSL_CTX structure to configure with the maximum protocol version
- `version`: The maximum TLS/SSL protocol version constant (e.g., TLS1_VERSION, TLS1_1_VERSION, TLS1_2_VERSION)

## Dependencies
- Functions called/Symbols referenced:
  - SSL_CTX_set_options (OpenSSL function)
  - SSL_OP_NO_TLSv1_1, SSL_OP_NO_TLSv1_2 (OpenSSL constants)
  - TLS1_1_VERSION, TLS1_2_VERSION (OpenSSL version constants)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - [be_tls_init](../b/be_tls_init.md) (src/backend/libpq/be-secure-openssl.c:234)
  - [initialize_SSL](../i/initialize_SSL.md) (src/interfaces/libpq/fe-secure-openssl.c:1009)

## Notes and Other Information
- This is a compatibility function only compiled when the native OpenSSL function is not available (pre-1.1.0)
- The function includes safety checks to prevent compilation on OpenSSL versions that support TLS 1.3, as those should have the native function
- Returns 1 on success, 0 on failure (when required SSL options are not available)
- Includes an assertion that the version parameter is not zero
- Does not automatically disable older insecure protocols like SSLv2/SSLv3 (unlike the min_proto_version function)
- The implementation uses conditional compilation to handle different OpenSSL versions gracefully
- Located in src/common/protocol_openssl.c:80-117