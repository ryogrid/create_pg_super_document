# be_tls_init

## Location
[src/backend/libpq/be-secure-openssl.c:98-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L98-L425)

## Overview
Initializes the OpenSSL TLS/SSL subsystem for PostgreSQL server connections, creating and configuring an SSL context with certificates, keys, protocol versions, and security settings.

## Definition

```c
int
be_tls_init(bool isServerStart)
```
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
  - [initialize_dh](../i/initialize_dh.md) / initialize_ecdh (key exchange setup)
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

## Simplified Source

```c
// Simplified version of be_tls_init
int be_tls_init(bool isServerStart) {
    SSL_CTX *context;
    int ssl_ver_min = -1;
    int ssl_ver_max = -1;

    // Initialize OpenSSL library (one-time setup)
    if (!SSL_initialized) {
        OPENSSL_init_ssl(OPENSSL_INIT_LOAD_CONFIG, NULL);
        SSL_initialized = true;
    }

    // Create new SSL context for configuration
    context = SSL_CTX_new(SSLv23_method());
    if (!context) {
        report_ssl_error("could not create SSL context", isServerStart);
        goto error;
    }

    // Configure SSL context mode and call initialization hook
    SSL_CTX_set_mode(context, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    (*openssl_tls_init_hook)(context, isServerStart);
    ssl_is_server_start = isServerStart;

    // Load server certificate and private key
    if (SSL_CTX_use_certificate_chain_file(context, ssl_cert_file) != 1) {
        report_ssl_error("could not load server certificate", isServerStart);
        goto error;
    }

    if (!check_ssl_key_file_permissions(ssl_key_file, isServerStart))
        goto error;

    if (SSL_CTX_use_PrivateKey_file(context, ssl_key_file, SSL_FILETYPE_PEM) != 1) {
        report_ssl_error("could not load private key", isServerStart);
        goto error;
    }

    if (SSL_CTX_check_private_key(context) != 1) {
        report_ssl_error("private key validation failed", isServerStart);
        goto error;
    }

    // Configure SSL/TLS protocol versions
    if (ssl_min_protocol_version) {
        ssl_ver_min = ssl_protocol_version_to_openssl(ssl_min_protocol_version);
        if (ssl_ver_min == -1 || !SSL_CTX_set_min_proto_version(context, ssl_ver_min)) {
            report_ssl_error("could not set minimum protocol version", isServerStart);
            goto error;
        }
    }

    if (ssl_max_protocol_version) {
        ssl_ver_max = ssl_protocol_version_to_openssl(ssl_max_protocol_version);
        if (ssl_ver_max == -1 || !SSL_CTX_set_max_proto_version(context, ssl_ver_max)) {
            report_ssl_error("could not set maximum protocol version", isServerStart);
            goto error;
        }
    }

    // Validate protocol version compatibility
    if (ssl_min_protocol_version && ssl_max_protocol_version && ssl_ver_min > ssl_ver_max) {
        report_ssl_error("min protocol version cannot be higher than max", isServerStart);
        goto error;
    }

    // Configure security options (disable insecure features)
    SSL_CTX_set_options(context, SSL_OP_NO_TICKET | SSL_OP_NO_COMPRESSION);
    SSL_CTX_set_session_cache_mode(context, SSL_SESS_CACHE_OFF);

    // Disable renegotiation for security
#ifdef SSL_OP_NO_RENEGOTIATION
    SSL_CTX_set_options(context, SSL_OP_NO_RENEGOTIATION);
#endif

    // Set up cryptographic key exchange
    if (!initialize_dh(context, isServerStart) || !initialize_ecdh(context, isServerStart))
        goto error;

    // Configure cipher suites
    if (SSL_CTX_set_cipher_list(context, SSLCipherSuites) != 1) {
        report_ssl_error("could not set cipher list", isServerStart);
        goto error;
    }

    if (SSLPreferServerCiphers)
        SSL_CTX_set_options(context, SSL_OP_CIPHER_SERVER_PREFERENCE);

    // Load Certificate Authority for client verification
    if (ssl_ca_file[0]) {
        if (!load_ca_certificates(context, isServerStart))
            goto error;
    }

    // Load Certificate Revocation List if configured
    if (ssl_crl_file[0] || ssl_crl_dir[0]) {
        if (!load_certificate_revocation_list(context, isServerStart))
            goto error;
    }

    // Success: Replace existing SSL context
    if (SSL_context)
        SSL_CTX_free(SSL_context);

    SSL_context = context;
    ssl_loaded_verify_locations = (ssl_ca_file[0] != 0);

    return 0;

error:
    if (context)
        SSL_CTX_free(context);
    return -1;
}
```

Key simplifications made:
- Consolidated repetitive error handling into helper function calls
- Abstracted detailed OpenSSL version checks and conditional compilation
- Simplified complex CRL loading logic into helper function
- Removed verbose error message formatting for clarity
- Focused on the main execution path and core functionality
- Consolidated similar security option settings
- Abstracted CA certificate loading complexity