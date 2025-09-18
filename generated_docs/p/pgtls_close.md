# pgtls_close

## Location
src/interfaces/libpq/fe-secure-openssl.c: 1644 - 1720

## Overview
Closes and cleans up SSL/TLS connection resources, including SSL objects, certificates, engines, and cryptographic callbacks for a PostgreSQL connection.

## Definition
```c
void pgtls_close(PGconn *conn)
```

## Detailed Description
This function performs comprehensive cleanup of SSL/TLS resources associated with a PostgreSQL connection. It handles the proper shutdown sequence for SSL connections and manages the cleanup of cryptographic callbacks to prevent issues when the libpq library is unloaded.

The function operates in two modes:
1. **SSL mode cleanup**: When conn->ssl_in_use is true, it performs SSL shutdown, frees the SSL object, cleans up peer certificates, and handles SSL engine cleanup if enabled.
2. **Non-SSL mode cleanup**: When SSL is not in use but cryptographic callbacks were loaded, it still calls the cleanup function to remove crypto callbacks.

A key design consideration is the delayed cleanup of SSL system resources. The function uses destroy_ssl_system() at the end to ensure all SSL operations are completed before removing cryptographic callbacks, preventing race conditions and deadlocks.

## Parameters / Member Variables
- `conn`: PGconn connection object containing SSL state and resources to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - SSL_shutdown (OpenSSL graceful connection close)
  - SSL_free (OpenSSL SSL object cleanup)
  - X509_free (OpenSSL certificate cleanup)
  - ENGINE_finish / ENGINE_free (SSL engine cleanup, if USE_SSL_ENGINE defined)
  - destroy_ssl_system (libpq crypto callback cleanup)
- Called from:
  - pgtls_open_client (on connection failures)
  - open_client_SSL (on handshake failures)
  - pqsecure_close (general connection cleanup)
  - pgunlock_thread (thread cleanup)

## Notes and Other Information
- Sets conn->ssl_in_use = false and conn->ssl_handshake_started = false after cleanup
- Handles both successful SSL connections and failed connection attempts
- SSL engine support (USE_SSL_ENGINE) provides hardware security module cleanup
- Uses destroy_needed flag to defer destroy_ssl_system() call until all SSL operations complete
- Prevents race conditions by ensuring SSL calls finish before removing crypto callbacks
- Works correctly whether SSL was successfully established or not
- Sets conn->crypto_loaded = false after system cleanup to track callback state
- Safe to call multiple times on the same connection
- Critical for preventing memory leaks and callback function pointer issues during library unload