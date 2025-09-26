# initialize_ecdh

## Location
src/backend/libpq/be-secure-openssl.c: 1413 - 1454

## Overview
Configures Elliptic Curve Diffie-Hellman (ECDH) parameters for SSL/TLS connections, enabling ephemeral ECDH key exchange with improved performance over traditional DH.

## Definition


## Detailed Description
The  function sets up Elliptic Curve Diffie-Hellman parameters for ephemeral key exchange in SSL/TLS connections. ECDH provides the same perfect forward secrecy benefits as traditional DH but with significantly better performance and smaller key sizes due to the mathematical properties of elliptic curves.

The function operates by converting the configured curve name () to an OpenSSL numeric identifier (NID), creating an EC_KEY structure for that curve, and configuring the SSL context to use it for temporary key generation. The process is much simpler than traditional DH setup because elliptic curves require only a curve name rather than complex parameter generation.

The function sets the  option to ensure that ECDH keys are never reused across connections, maintaining perfect forward secrecy. The function is conditionally compiled and only operates when OpenSSL is built with ECDH support.

## Parameters / Member Variables
- : SSL context structure to configure with ECDH parameters
- : Boolean flag indicating whether this is called during server startup (affects error reporting severity)

## Dependencies
- Functions called/Symbols referenced:
  - OBJ_sn2nid (OpenSSL function to convert curve name to numeric ID)
  - SSLECDHCurve (PostgreSQL configuration variable for ECDH curve name)
  - EC_KEY_new_by_curve_name (OpenSSL function to create EC key for specified curve)
  - SSL_CTX_set_options (OpenSSL function to set SSL context options)
  - SSL_CTX_set_tmp_ecdh (OpenSSL function to set temporary ECDH parameters)
  - EC_KEY_free (OpenSSL function to free EC key structure)
  - ereport (PostgreSQL error reporting function)
- Called from (representative examples):
  - be_tls_init (SSL context initialization)

## Notes and Other Information
- ECDH setup is much simpler than traditional DH because it only requires specifying a curve name
- The function is conditionally compiled with  for compatibility
- Uses  option to prevent ECDH key reuse, maintaining perfect forward secrecy
- Error severity depends on  flag: FATAL during startup, LOG during reload
- EC_KEY structure is freed after configuration because OpenSSL makes an internal copy
- ECDH provides equivalent security to traditional DH with much better performance characteristics
- The curve name is validated through OpenSSL's object identifier system before use
- This functionality enables support for ECDH-based cipher suites in PostgreSQL's SSL implementation
- Modern PostgreSQL installations typically use ECDH instead of traditional DH for better performance