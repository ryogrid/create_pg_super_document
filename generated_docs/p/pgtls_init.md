# pgtls_init

## Location
[src/interfaces/libpq/fe-secure-openssl.c:769-854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L769-L854)

## Overview
Initializes the SSL/TLS library (OpenSSL) with proper thread safety mechanisms, setting up both libcrypto and libssl components as needed.

## Definition

```c
int
pgtls_init(PGconn *conn, bool do_ssl, bool do_crypto)
```
## Detailed Description
This function performs comprehensive initialization of OpenSSL libraries with thread safety support. It handles both the libcrypto (cryptographic functions) and libssl (SSL/TLS protocol) components of OpenSSL, with special attention to thread safety requirements in older OpenSSL versions.

The initialization process includes several key steps:

1. **Thread-safe library initialization**: Uses mutex protection to ensure library initialization is atomic
2. **Legacy threading support**: For older OpenSSL versions (< 1.1.0), sets up mutex arrays and callback functions required for thread safety
3. **Crypto library setup**: Initializes libcrypto with thread callbacks if the application hasn't already done so
4. **SSL library setup**: Initializes libssl, loading configuration and error strings as needed

The function respects application-level choices about which components to initialize, allowing fine-grained control over OpenSSL setup through the  and  parameters.

## Parameters / Member Variables
- : PostgreSQL connection object that tracks crypto library loading state
- : Boolean flag indicating whether to initialize the SSL library component
- : Boolean flag indicating whether to initialize the crypto library component

## Dependencies
- Functions called/Symbols referenced:
  - : Provides thread-safe initialization
  - : Allocates memory for mutex array
  - : Mutex type for thread synchronization
  - : Releases initialization mutex
  - : Initializes individual lock array mutexes
  - : Sets up thread ID callback for OpenSSL
  - : Sets up locking callback for OpenSSL
- Called from (representative examples):
  - : Main secure connection initialization
  - : Thread management context

## Notes and Other Information
- Return value: 0 for success, -1 for failure
- Uses conditional compilation for legacy OpenSSL support ()
- Manages global state including  and  counters
- Allocates and manages a global mutex array () for thread safety
- Only sets OpenSSL callbacks if they haven't already been set by the application
- Supports both modern OpenSSL (1.1.0+) and legacy versions with different initialization APIs
- Located in 
- Thread-safe initialization prevents race conditions during library setup