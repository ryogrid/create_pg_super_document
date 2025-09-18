# destroy_ssl_system

## Location
src/interfaces/libpq/fe-secure-openssl.c: 855 - 897

## Overview
Cleans up SSL/TLS system resources when the last libpq connection is closed, ensuring callback functions are unregistered to prevent issues when the libpq library is unloaded.

## Definition
```c
static void destroy_ssl_system(void)
```

## Detailed Description
This function handles the cleanup of OpenSSL/libcrypto callback functions when libpq connections are closed. It's specifically designed to prevent callback function pointer issues that could occur if the libpq library is dynamically unloaded while other parts of the system continue using libcrypto.

The function decrements a connection counter and when it reaches zero (indicating no active SSL connections remain), it unregisters the custom locking and thread ID callbacks that were previously set up for thread-safe OpenSSL operations. This cleanup is only necessary when compiled in threadsafe mode and is not needed for OpenSSL 1.1.0 and later versions.

The function deliberately does not free the lock array to allow reuse if new connections are established later in the same process, though this results in a small memory leak on repeated library load/unload cycles.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)
  - [pthread_mutex_unlock](../p/pthread_mutex_unlock.md)
  - pq_lockingcallback
  - pq_threadidcallback
  - CRYPTO_get_locking_callback (OpenSSL)
  - CRYPTO_set_locking_callback (OpenSSL)
  - CRYPTO_get_id_callback (OpenSSL)
  - CRYPTO_set_id_callback (OpenSSL)
- Called from:
  - [pgtls_close](../p/pgtls_close.md)

## Notes and Other Information
- Only compiled when HAVE_CRYPTO_LOCK is defined (threadsafe mode)
- Uses ssl_config_mutex for thread-safe access to global state
- Maintains crypto_open_connections counter to track active SSL connections
- Callback cleanup only occurs if no other code has replaced the callbacks in the meantime
- Memory leak is intentional to allow callback reuse in subsequent connections
- Not needed for OpenSSL 1.1.0+ which handles threading internally