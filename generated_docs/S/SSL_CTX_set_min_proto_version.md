# SSL_CTX_set_min_proto_version

## Location
[src/common/protocol_openssl.c:41-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/protocol_openssl.c#L41-L79)

## Overview
Sets the minimum SSL/TLS protocol version for an SSL context by disabling older protocol versions through SSL options.

## Definition
```c
int SSL_CTX_set_min_proto_version(SSL_CTX *ctx, int version)
```

## Detailed Description
This function is a compatibility implementation for older OpenSSL versions that lack the native SSL_CTX_set_min_proto_version function (introduced in OpenSSL 1.1.0). It works by setting SSL options to disable protocol versions older than the specified minimum version. The function systematically disables SSLv2, SSLv3, and optionally TLS 1.0, 1.1, and 1.2 based on the requested minimum version.

The implementation uses conditional compilation to handle different OpenSSL versions that may define TLS version macros without corresponding SSL_OP_NO_* options. If a required SSL option is not available, the function returns failure (0).

## Parameters / Member Variables
- `ctx`: The SSL_CTX structure to configure with the minimum protocol version
- `version`: The minimum TLS/SSL protocol version constant (e.g., TLS1_VERSION, TLS1_1_VERSION, TLS1_2_VERSION)

## Dependencies
- Functions called/Symbols referenced:
  - SSL_CTX_set_options (OpenSSL function)
  - SSL_OP_NO_SSLv2, SSL_OP_NO_SSLv3, SSL_OP_NO_TLSv1, SSL_OP_NO_TLSv1_1, SSL_OP_NO_TLSv1_2 (OpenSSL constants)
  - TLS1_VERSION, TLS1_1_VERSION, TLS1_2_VERSION (OpenSSL version constants)
- Called from (representative examples):
  - [be_tls_init](../b/be_tls_init.md) (src/backend/libpq/be-secure-openssl.c:211)
  - [initialize_SSL](../i/initialize_SSL.md) (src/interfaces/libpq/fe-secure-openssl.c:983)

## Notes and Other Information
- This is a compatibility function only compiled when the native OpenSSL function is not available (pre-1.1.0)
- The function includes safety checks to prevent compilation on OpenSSL versions that support TLS 1.3, as those should have the native function
- Returns 1 on success, 0 on failure (when required SSL options are not available)
- Always disables SSLv2 and SSLv3 regardless of the requested minimum version for security reasons
- Located in src/common/protocol_openssl.c:41-79