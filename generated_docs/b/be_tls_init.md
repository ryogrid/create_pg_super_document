# be_tls_init

## Location
src/backend/libpq/be-secure-openssl.c: 98 - 425

## Overview
Initializes the OpenSSL TLS/SSL subsystem for PostgreSQL server connections, creating and configuring an SSL context with certificates, keys, protocol versions, and security settings.

## Definition


## Detailed Description
The  function performs comprehensive SSL/TLS initialization for PostgreSQL's backend server. It creates and configures an SSL context that will be used for secure client connections. The function handles:

1. **OpenSSL Library Initialization**: Initializes the OpenSSL library if not already done, loading configuration and error strings
2. **SSL Context Creation**: Creates a new SSL context using SSLv23_method() to support negotiation of the highest mutually supported protocol version
3. **Certificate and Key Loading**: Loads the server's certificate chain and private key files, verifying their validity and compatibility
4. **Protocol Version Configuration**: Sets minimum and maximum SSL/TLS protocol versions based on configuration parameters
5. **Security Options**: Configures various security settings including disabling session tickets, compression, renegotiation, and session caching
6. **Cipher Suite Configuration**: Sets up allowed cipher suites and server cipher preference
7. **Certificate Authority Setup**: Loads CA certificates for client certificate verification if configured
8. **Certificate Revocation List**: Loads CRL files/directories for certificate revocation checking

The function uses either FATAL or LOG level error reporting depending on whether this is server startup (isServerStart=true) or runtime reconfiguration.

## Parameters / Member Variables
- : Boolean flag indicating whether this is being called during server startup (true) or during runtime reconfiguration (false). Affects error reporting level - FATAL errors during startup, LOG level during runtime.

## Dependencies
- Functions called/Symbols referenced:
  - OPENSSL_init_ssl / SSL_library_init (OpenSSL initialization)
  - SSL_CTX_new (SSL context creation)
  - SSL_CTX_use_certificate_chain_file (certificate loading)
  - SSL_CTX_use_PrivateKey_file (private key loading)
  - SSL_CTX_check_private_key (key validation)
  - [ssl_protocol_version_to_openssl](../s/ssl_protocol_version_to_openssl.md) (protocol version conversion)
  - [SSL_CTX_set_min_proto_version](../S/SSL_CTX_set_min_proto_version.md) / SSL_CTX_set_max_proto_version (protocol configuration)
  - initialize_dh / initialize_ecdh (key exchange setup)
  - SSL_CTX_set_cipher_list (cipher configuration)
  - SSL_CTX_load_verify_locations (CA certificate loading)
  - [check_ssl_key_file_permissions](../c/check_ssl_key_file_permissions.md) (key file security check)
  - [SSLerrmessage](../S/SSLerrmessage.md) (error message formatting)
  - [verify_cb](../v/verify_cb.md) (certificate verification callback)

- Called from (representative examples):
  - [secure_initialize](../s/secure_initialize.md) (in be-secure.c:76)

## Notes and Other Information
- The function maintains a global SSL_context that is replaced on successful reconfiguration
- OpenSSL is initialized only once using the SSL_initialized static flag
- The function supports both server startup and runtime SSL reconfiguration scenarios
- Error handling differs based on context: FATAL during startup prevents server start, LOG during runtime allows continued operation
- The SSL context is configured with security-focused defaults: no compression, no renegotiation, no session caching/tickets
- Certificate and key file permissions are validated for security
- The function supports loading Certificate Revocation Lists (CRL) for enhanced security
- Protocol version compatibility is checked when both min and max versions are specified
- The SSL context ownership model ensures proper memory management and cleanup