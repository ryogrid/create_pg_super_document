# ClientAuthentication

## Location
[src/backend/libpq/auth.c:390-510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L390-L510)

## Overview
The main entry point function for client authentication in PostgreSQL that orchestrates the entire authentication process and determines access based on pg_hba.conf rules.

## Definition

```c
void
ClientAuthentication(Port *port)
```
## Detailed Description
 is the central function responsible for authenticating client connections in PostgreSQL. It serves as the dispatcher that determines the appropriate authentication method based on pg_hba.conf configuration and coordinates the entire authentication process.

The function first retrieves the authentication method for the specific client/database combination using the HBA (Host-Based Authentication) system. It then performs pre-authentication checks such as client certificate validation if required, followed by method-specific authentication logic through a comprehensive switch statement.

The function handles all authentication methods supported by PostgreSQL including password-based authentication (MD5, SCRAM, plain password), external authentication systems (PAM, LDAP, RADIUS, Kerberos/GSS, SSPI), identity-based authentication (Ident, Peer), certificate-based authentication, and trust/reject policies.

If authentication succeeds, it sends an AUTH_REQ_OK message to the client. If it fails, it calls auth_failed() to terminate the connection with appropriate error messaging. The function never returns on authentication failure as the backend process is terminated.

## Parameters / Member Variables
- `*port`: Pointer to Port structure containing all connection information, authentication state, and HBA configuration
## Dependencies
- Functions called/Symbols referenced:
  - [hba_getauthmethod](../h/hba_getauthmethod.md) (retrieve authentication method from HBA rules)
  - [secure_loaded_verify_locations](../s/secure_loaded_verify_locations.md) (check SSL certificate store)
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md) (resolve client address information)
  - [sendAuthRequest](../s/sendAuthRequest.md) (send authentication requests/responses to client)
  - [auth_failed](../a/auth_failed.md) (handle authentication failure and termination)
- Authentication method handlers:
  - [pg_GSS_checkauth](../p/pg_GSS_checkauth.md), pg_GSS_recvauth (GSS/Kerberos authentication)
  - [pg_SSPI_recvauth](../p/pg_SSPI_recvauth.md) (Windows SSPI authentication)
  - [auth_peer](../a/auth_peer.md) (Peer authentication)
  - [ident_inet](../i/ident_inet.md) (Ident authentication)  
  - [CheckPWChallengeAuth](CheckPWChallengeAuth.md) (MD5/SCRAM password authentication)
  - [CheckPasswordAuth](CheckPasswordAuth.md) (plain password authentication)
  - [CheckPAMAuth](CheckPAMAuth.md) (PAM authentication)
  - [CheckBSDAuth](CheckBSDAuth.md) (BSD authentication)
  - [CheckLDAPAuth](CheckLDAPAuth.md) (LDAP authentication)
  - [CheckRADIUSAuth](CheckRADIUSAuth.md) (RADIUS authentication)
  - [CheckCertAuth](CheckCertAuth.md) (certificate authentication)
- Called from:
  - [PerformAuthentication](../P/PerformAuthentication.md) (main authentication entry point)

## Notes and Other Information
- The function never returns on authentication failure - the process is terminated
- Supports a ClientAuthentication_hook for extensions to customize authentication behavior  
- Handles both regular database connections and replication connections with method-specific error messages
- Implements detailed logging for connection attempts when Log_connections is enabled
- Certificate validation is performed both before and after method-specific authentication for verify-full and cert methods
- Uses HOSTNAME_LOOKUP_DETAIL macro to provide detailed hostname resolution information in error messages
- Special handling for implicit vs explicit reject entries in pg_hba.conf with different error messages
- Authentication method constants (uaTrust, uaReject, etc.) determine the specific authentication logic path

## Simplified Source

```c
// Simplified version of ClientAuthentication
void ClientAuthentication(Port *port) {
    int status = STATUS_ERROR;
    const char *logdetail = NULL;

    // Step 1: Get authentication method from pg_hba.conf
    hba_getauthmethod(port);
    CHECK_FOR_INTERRUPTS();

    // Step 2: Pre-authentication client certificate checks
    if (port->hba->clientcert != clientCertOff) {
        if (!secure_loaded_verify_locations())
            ereport(FATAL, "client certificates require root certificate store");

        if (!port->peer_cert_valid)
            ereport(FATAL, "connection requires valid client certificate");
    }

    // Step 3: Execute authentication method based on HBA configuration
    switch (port->hba->auth_method) {
        case uaReject:
            // Explicit reject in pg_hba.conf - report detailed error and terminate
            ereport(FATAL, "pg_hba.conf explicitly rejects connection");
            break;

        case uaImplicitReject:
            // No matching pg_hba.conf entry - report detailed error and terminate
            ereport(FATAL, "no pg_hba.conf entry for connection");
            break;

        case uaGSS:
            // GSS/Kerberos authentication
            setup_gss_context(port);
            status = perform_gss_authentication(port);
            break;

        case uaSSPI:
            // Windows SSPI authentication
            setup_gss_context(port);
            status = pg_SSPI_recvauth(port);
            break;

        case uaPeer:
            // Peer authentication (Unix socket credentials)
            status = auth_peer(port);
            break;

        case uaIdent:
            // Ident authentication (TCP ident protocol)
            status = ident_inet(port);
            break;

        case uaMD5:
        case uaSCRAM:
            // Challenge-response password authentication
            status = CheckPWChallengeAuth(port, &logdetail);
            break;

        case uaPassword:
            // Plain password authentication
            status = CheckPasswordAuth(port, &logdetail);
            break;

        case uaPAM:
            // PAM authentication
            status = CheckPAMAuth(port, port->user_name, "");
            break;

        case uaBSD:
            // BSD authentication
            status = CheckBSDAuth(port, port->user_name);
            break;

        case uaLDAP:
            // LDAP authentication
            status = CheckLDAPAuth(port);
            break;

        case uaRADIUS:
            // RADIUS authentication
            status = CheckRADIUSAuth(port);
            break;

        case uaCert:
        case uaTrust:
            // Certificate or trust authentication - accept connection
            status = STATUS_OK;
            break;
    }

    // Step 4: Post-authentication certificate verification if required
    if ((status == STATUS_OK && port->hba->clientcert == clientCertFull) ||
        port->hba->auth_method == uaCert) {
        status = CheckCertAuth(port);
    }

    // Step 5: Log successful connections if enabled
    if (Log_connections && status == STATUS_OK && !MyClientConnectionInfo.authn_id) {
        ereport(LOG, "connection authenticated: user=\"%s\" method=%s",
                port->user_name, hba_authname(port->hba->auth_method));
    }

    // Step 6: Execute authentication hook if configured
    if (ClientAuthentication_hook)
        (*ClientAuthentication_hook)(port, status);

    // Step 7: Send final response to client
    if (status == STATUS_OK)
        sendAuthRequest(port, AUTH_REQ_OK, NULL, 0);
    else
        auth_failed(port, status, logdetail);  // Terminates process on failure
}
```

Key simplifications made:
- Removed detailed error message construction and hostname resolution logic
- Abstracted platform-specific conditional compilation blocks
- Simplified memory context allocation for GSS structures
- Consolidated similar authentication method patterns
- Reduced complex macro expansions to simple function calls
- Streamlined error reporting to essential error cases
- Removed detailed encryption state detection logic
- Abstracted complex ereport calls to simplified versions