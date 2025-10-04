# initialize_SSL

## Location
[src/interfaces/libpq/fe-secure-openssl.c:898-1479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L898-L1479)

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
  - [pqGetHomeDirectory](../p/pqGetHomeDirectory.md)
  - [SSLerrmessage](../S/SSLerrmessage.md) / SSLerrfree (error handling)
  - SSL_CTX_new / SSL_CTX_free (OpenSSL context management)
  - SSL_new / my_SSL_set_fd (SSL object creation)
  - SSL_CTX_use_certificate_chain_file (certificate loading)
  - SSL_use_PrivateKey_file (private key loading)
  - [ssl_protocol_version_to_openssl](../s/ssl_protocol_version_to_openssl.md) (version conversion)
  - [cert_cb](../c/cert_cb.md) / PQssl_passwd_cb / verify_cb (callback functions)
  - Various OpenSSL configuration functions
- Called from:
  - [pgtls_open_client](../p/pgtls_open_client.md)

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

## Simplified Source
```c
static int initialize_SSL(PGconn *conn) {
    SSL_CTX *SSL_context;
    char homedir[MAXPGPATH];
    char fnbuf[MAXPGPATH];
    bool have_homedir;
    bool have_cert;
    bool have_rootcert;

    // Get home directory if needed for default certificate paths
    have_homedir = pqGetHomeDirectory(homedir, sizeof(homedir));

    // Create new SSL context for this connection
    SSL_context = SSL_CTX_new(SSLv23_method());
    if (!SSL_context)
        return -1;

    // Set up password callback if needed
    if (PQsslKeyPassHook || (conn->sslpassword && strlen(conn->sslpassword) > 0)) {
        SSL_CTX_set_default_passwd_cb(SSL_context, PQssl_passwd_cb);
        SSL_CTX_set_default_passwd_cb_userdata(SSL_context, conn);
    }

    // Disable insecure protocols
    SSL_CTX_set_options(SSL_context, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

    // Set protocol version constraints if specified
    if (conn->ssl_min_protocol_version && strlen(conn->ssl_min_protocol_version) > 0) {
        int ssl_min_ver = ssl_protocol_version_to_openssl(conn->ssl_min_protocol_version);
        if (ssl_min_ver == -1 || !SSL_CTX_set_min_proto_version(SSL_context, ssl_min_ver)) {
            SSL_CTX_free(SSL_context);
            return -1;
        }
    }

    if (conn->ssl_max_protocol_version && strlen(conn->ssl_max_protocol_version) > 0) {
        int ssl_max_ver = ssl_protocol_version_to_openssl(conn->ssl_max_protocol_version);
        if (ssl_max_ver == -1 || !SSL_CTX_set_max_proto_version(SSL_context, ssl_max_ver)) {
            SSL_CTX_free(SSL_context);
            return -1;
        }
    }

    // Load root certificates
    if (conn->sslrootcert && strlen(conn->sslrootcert) > 0)
        strlcpy(fnbuf, conn->sslrootcert, sizeof(fnbuf));
    else if (have_homedir)
        snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, ROOT_CERT_FILE);
    else
        fnbuf[0] = '\0';

    if (strcmp(fnbuf, "system") == 0) {
        // Use system root certificates
        if (SSL_CTX_set_default_verify_paths(SSL_context) != 1) {
            SSL_CTX_free(SSL_context);
            return -1;
        }
        have_rootcert = true;
    } else if (fnbuf[0] != '\0' && stat(fnbuf, &buf) == 0) {
        // Load specific root certificate file
        if (SSL_CTX_load_verify_locations(SSL_context, fnbuf, NULL) != 1) {
            SSL_CTX_free(SSL_context);
            return -1;
        }
        have_rootcert = true;
    } else {
        // No root cert - verify mode determines if this is an error
        if (conn->sslmode[0] == 'v') { // verify-ca or verify-full
            SSL_CTX_free(SSL_context);
            return -1;
        }
        have_rootcert = false;
    }

    // Load client certificate
    if (conn->sslcert && strlen(conn->sslcert) > 0)
        strlcpy(fnbuf, conn->sslcert, sizeof(fnbuf));
    else if (have_homedir)
        snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, USER_CERT_FILE);
    else
        fnbuf[0] = '\0';

    if (conn->sslcertmode[0] == 'd') { // disabled
        have_cert = false;
    } else if (fnbuf[0] != '\0' && stat(fnbuf, &buf) == 0) {
        // Load client certificate
        if (SSL_CTX_use_certificate_chain_file(SSL_context, fnbuf) != 1) {
            SSL_CTX_free(SSL_context);
            return -1;
        }
        have_cert = true;
    } else {
        have_cert = false;
    }

    // Create SSL object and associate with connection
    if (!(conn->ssl = SSL_new(SSL_context)) ||
        !SSL_set_app_data(conn->ssl, conn) ||
        !my_SSL_set_fd(conn, conn->sock)) {
        SSL_CTX_free(SSL_context);
        return -1;
    }
    conn->ssl_in_use = true;

    // Free SSL context (SSL object holds reference)
    SSL_CTX_free(SSL_context);

    // Set SNI if enabled and hostname is not IP address
    if (conn->sslsni && conn->sslsni[0] == '1') {
        const char *host = conn->connhost[conn->whichhost].host;
        if (host && host[0] && !is_ip_address(host)) {
            if (SSL_set_tlsext_host_name(conn->ssl, host) != 1)
                return -1;
        }
    }

    // Set ALPN
    if (SSL_set_alpn_protos(conn->ssl, alpn_protos, sizeof(alpn_protos)) != 0)
        return -1;

    // Load private key if we have a certificate
    if (have_cert) {
        // Determine key file path
        if (conn->sslkey && strlen(conn->sslkey) > 0)
            strlcpy(fnbuf, conn->sslkey, sizeof(fnbuf));
        else if (have_homedir)
            snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, USER_KEY_FILE);

        // Load private key (try PEM first, then DER)
        if (SSL_use_PrivateKey_file(conn->ssl, fnbuf, SSL_FILETYPE_PEM) != 1) {
            if (SSL_use_PrivateKey_file(conn->ssl, fnbuf, SSL_FILETYPE_ASN1) != 1)
                return -1;
        }

        // Verify certificate and key match
        if (SSL_check_private_key(conn->ssl) != 1)
            return -1;
    }

    // Set verification callback if root cert loaded
    if (have_rootcert)
        SSL_set_verify(conn->ssl, SSL_VERIFY_PEER, verify_cb);

    // Configure compression
    if (conn->sslcompression && conn->sslcompression[0] == '0')
        SSL_set_options(conn->ssl, SSL_OP_NO_COMPRESSION);
    else
        SSL_clear_options(conn->ssl, SSL_OP_NO_COMPRESSION);

    return 0;
}
```