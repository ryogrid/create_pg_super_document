# ClientAuthentication

## Location
src/backend/libpq/auth.c: 390 - 510

## Overview
The main entry point function for client authentication in PostgreSQL that orchestrates the entire authentication process and determines access based on pg_hba.conf rules.

## Definition


## Detailed Description
 is the central function responsible for authenticating client connections in PostgreSQL. It serves as the dispatcher that determines the appropriate authentication method based on pg_hba.conf configuration and coordinates the entire authentication process.

The function first retrieves the authentication method for the specific client/database combination using the HBA (Host-Based Authentication) system. It then performs pre-authentication checks such as client certificate validation if required, followed by method-specific authentication logic through a comprehensive switch statement.

The function handles all authentication methods supported by PostgreSQL including password-based authentication (MD5, SCRAM, plain password), external authentication systems (PAM, LDAP, RADIUS, Kerberos/GSS, SSPI), identity-based authentication (Ident, Peer), certificate-based authentication, and trust/reject policies.

If authentication succeeds, it sends an AUTH_REQ_OK message to the client. If it fails, it calls auth_failed() to terminate the connection with appropriate error messaging. The function never returns on authentication failure as the backend process is terminated.

## Parameters / Member Variables
- : Pointer to Port structure containing all connection information, authentication state, and HBA configuration

## Dependencies
- Functions called/Symbols referenced:
  - [hba_getauthmethod](../h/hba_getauthmethod.md) (retrieve authentication method from HBA rules)
  - [secure_loaded_verify_locations](../s/secure_loaded_verify_locations.md) (check SSL certificate store)
  - pg_getnameinfo_all (resolve client address information)
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