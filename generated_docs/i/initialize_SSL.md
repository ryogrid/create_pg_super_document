# initialize_SSL

## Location
src/interfaces/libpq/fe-secure-openssl.c: 898 - 1479

## Overview
Creates and configures a per-connection SSL object with client certificates, private keys, and trusted CA certificates for establishing secure TLS/SSL connections in libpq.

## Definition
```c
static int initialize_SSL(PGconn *conn)
```

## Detailed Description
This function performs comprehensive SSL/TLS initialization for a PostgreSQL client connection. It creates a new SSL context for each connection (avoiding certificate conflicts), loads client certificates and private keys, configures root certificate verification, and sets up various SSL options like protocol versions, SNI, and ALPN.

The function handles multiple certificate sources including files, system certificate stores, and SSL engines. It validates file permissions for security, supports both PEM and DER certificate formats, and performs extensive error checking throughout the process.

Key features include:
- Per-connection SSL context creation to avoid certificate sharing issues
- Support for system root certificates via "system" sentinel value
- Client certificate validation and private key verification
- SSL engine support for hardware security modules
- Protocol version constraints (min/max SSL/TLS versions)
- Server Name Indication (SNI) configuration
- Application Layer Protocol Negotiation (ALPN) setup
- Certificate Revocation List (CRL) processing
- Comprehensive file permission validation for security

## Parameters / Member Variables
- `conn`: PGconn connection object containing SSL configuration parameters and storing the created SSL object

## Dependencies
- Functions called/Symbols referenced:
  - pqGetHomeDirectory
  - SSLerrmessage / SSLerrfree (error handling)
  - SSL_CTX_new / SSL_CTX_free (OpenSSL context management)
  - SSL_new / my_SSL_set_fd (SSL object creation)
  - SSL_CTX_use_certificate_chain_file (certificate loading)
  - SSL_use_PrivateKey_file (private key loading)
  - ssl_protocol_version_to_openssl (version conversion)
  - cert_cb / PQssl_passwd_cb / verify_cb (callback functions)
  - Various OpenSSL configuration functions
- Called from:
  - pgtls_open_client

## Notes and Other Information
- Creates separate SSL context for each connection to avoid certificate conflicts
- Returns 0 on success, -1 on failure (with error message in conn->errorMessage)
- Supports loading certificates from default locations (~/.postgresql/) or custom paths
- Validates private key file permissions (0600 for user-owned, 0640 for root-owned files)
- Falls back from PEM to DER format when loading private keys
- SSL engine support allows using hardware security modules for private keys
- Disables SSLv2 and SSLv3 for security, supports configurable min/max protocol versions
- SNI is set automatically for hostname-based connections (not IP addresses)
- ALPN is configured for protocol negotiation
- Root certificate verification can use system stores or custom CA files
- CRL (Certificate Revocation List) support is optional and configured silently