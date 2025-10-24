# pgtls_init_library

## Location
[src/interfaces/libpq/fe-secure-openssl.c:104-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L104-L117)

## Overview
Initializes the PostgreSQL TLS library configuration by setting flags for SSL and crypto library initialization, with protection against changes while connections are active.

## Definition

```c
void
pgtls_init_library(bool do_ssl, int do_crypto)
```
## Detailed Description
This function configures the global SSL and crypto library initialization flags for the PostgreSQL libpq client library. It serves as a safety mechanism to control which parts of the OpenSSL library should be initialized by PostgreSQL versus the application. The function includes a critical safety check that prevents modification of these flags when there are active SSL connections, which would lead to inconsistent library state.

The function is designed to be called early in application startup, typically through PQinitSSL() or PQinitOpenSSL() wrapper functions, before any SSL connections are established.

## Parameters / Member Variables
- `do_ssl`: Boolean flag indicating whether PostgreSQL should initialize the SSL library portion of OpenSSL
- `do_crypto`: Integer flag indicating whether PostgreSQL should initialize the crypto library portion of OpenSSL (typically treated as boolean)

## Dependencies
- Functions called/Symbols referenced:
  - crypto_open_connections (global variable)
  - pq_init_ssl_lib (global variable)
  - pq_init_crypto_lib (global variable)
- Called from (representative examples):
  - [PQinitSSL](../P/PQinitSSL.md) (in fe-secure.c:118)
  - [PQinitOpenSSL](../P/PQinitOpenSSL.md) (in fe-secure.c:130)
  - pgunlock_thread (referenced in libpq-int.h:788)

## Notes and Other Information
- The function performs no action if crypto_open_connections is non-zero, providing thread-safe protection against runtime configuration changes
- This is part of the OpenSSL frontend secure connection implementation
- The separation of SSL and crypto library initialization allows applications to have fine-grained control over OpenSSL initialization when integrating with other libraries that also use OpenSSL
- Global variables pq_init_ssl_lib and pq_init_crypto_lib default to true, meaning PostgreSQL will initialize both SSL and crypto libraries by default
- Location: src/interfaces/libpq/fe-secure-openssl.c:104-115

## Simplified Source

```c
void pgtls_init_library(bool do_ssl, int do_crypto) {
    // Prevent changes while connections are active
    if (crypto_open_connections != 0)
        return;

    // Set SSL and crypto library initialization flags
    pq_init_ssl_lib = do_ssl;
    pq_init_crypto_lib = do_crypto;
}
```