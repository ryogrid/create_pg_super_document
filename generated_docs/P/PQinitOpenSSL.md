# PQinitOpenSSL

## Location
src/interfaces/libpq/fe-secure.c: 127 - 137

## Overview
Provides fine-grained control over OpenSSL and libcrypto library initialization for client applications.

## Definition
```c
void PQinitOpenSSL(int do_ssl, int do_crypto)
```

## Detailed Description
PQinitOpenSSL is an exported function that allows applications to have granular control over SSL library initialization. Unlike PQinitSSL which controls both SSL and crypto libraries together, this function allows separate control over OpenSSL and libcrypto initialization. This is useful for applications that may have already initialized one library but not the other, or need different initialization strategies for each component.

## Parameters / Member Variables
- `do_ssl`: Integer flag indicating whether to initialize the SSL library (non-zero to initialize, 0 to skip)
- `do_crypto`: Integer flag indicating whether to initialize the crypto library (non-zero to initialize, 0 to skip)

## Dependencies
- Functions called/Symbols referenced:
  - [pgtls_init_library](../p/pgtls_init_library.md)
  - USE_SSL (conditional compilation flag)
- Called from (representative examples):
  - Referenced in PQsetdb header (src/interfaces/libpq/libpq-fe.h:421)

## Notes and Other Information
- Only has effect when PostgreSQL is compiled with SSL support (USE_SSL defined)
- When USE_SSL is not defined, this function becomes a no-op
- Provides more granular control compared to PQinitSSL by allowing separate SSL and crypto library initialization flags
- Applications should call this before making any PostgreSQL connections if they want to control SSL initialization
- Useful when applications have complex SSL/crypto initialization requirements or are using multiple SSL-enabled libraries