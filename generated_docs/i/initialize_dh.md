# initialize_dh

## Location
[src/backend/libpq/be-secure-openssl.c:1375-1412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1375-L1412)

## Overview
Configures Diffie-Hellman (DH) parameters for SSL/TLS connections, enabling ephemeral DH key exchange for perfect forward secrecy.

## Definition

```c
static bool
initialize_dh(SSL_CTX *context, bool isServerStart)
```
## Detailed Description
The  function sets up Diffie-Hellman parameters required for ephemeral DH key exchange in SSL/TLS connections. DH parameters are mathematical constants used to generate temporary DH keys during the handshake process, providing perfect forward secrecy by ensuring that each session uses unique encryption keys that cannot be recovered even if the server's private key is compromised.

The function first attempts to load custom DH parameters from a file specified by . If no custom file is configured or loading fails, it falls back to built-in DH parameters () provided by OpenSSL. The function also sets the  option to ensure that DH keys are never reused across connections.

Since DH parameter generation is computationally expensive, the parameters are precomputed and loaded rather than generated dynamically. The loaded parameters are applied to the SSL context and then freed, as OpenSSL maintains an internal copy.

## Parameters / Member Variables
- : SSL context structure to configure with DH parameters
- : Boolean flag indicating whether this is called during server startup (affects error reporting severity)

## Dependencies
- Functions called/Symbols referenced:
  - SSL_CTX_set_options (OpenSSL function to set SSL options)
  - load_dh_file (PostgreSQL function to load DH parameters from file)
  - load_dh_buffer (PostgreSQL function to load DH parameters from buffer)
  - FILE_DH2048 (Built-in DH parameters constant)
  - SSL_CTX_set_tmp_dh (OpenSSL function to set temporary DH parameters)
  - SSLerrmessage (PostgreSQL function to format SSL error messages)
  - DH_free (OpenSSL function to free DH structure)
  - ereport (PostgreSQL error reporting function)
- Called from (representative examples):
  - be_tls_init (SSL context initialization)

## Notes and Other Information
- DH parameters can take significant time to compute, so precomputation is essential for performance
- The function provides fallback to OpenSSL project's standard DH parameters if custom parameters are unavailable
- Uses  option to prevent DH key reuse, maintaining perfect forward secrecy
- Error severity depends on  flag: FATAL during startup, LOG during reload
- The DH structure is freed after setting parameters because OpenSSL makes an internal copy
- This functionality is critical for supporting DH-based cipher suites in PostgreSQL's SSL implementation
- Built-in parameters use 2048-bit DH group for strong security while maintaining reasonable performance