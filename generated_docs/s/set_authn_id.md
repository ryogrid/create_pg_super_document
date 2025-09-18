# set_authn_id

## Location
src/backend/libpq/auth.c: 352 - 389

## Overview
Sets the authenticated identity for the current user connection and stores it alongside the authentication method in the client connection info structure.

## Definition


## Detailed Description
The  function is responsible for recording the authenticated identity of a user once authentication has been successfully completed. This function stores the authentication identifier in the global  structure and associates it with the authentication method used.

The function implements important security safeguards by ensuring that the authentication ID can only be set once per connection. If multiple authentication providers attempt to set the ID, the function treats this as a fatal error to prevent authentication conflicts.

The authenticated identity and method are logged if connection logging is enabled, providing administrators with audit trail information about successful authentications.

## Parameters / Member Variables
- : Pointer to the Port structure containing connection and HBA information
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): String containing the authenticated identity (will be copied to TopMemoryContext)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextStrdup (for copying the ID string to permanent memory)
  - ereport (for logging and error reporting)
  - errdetail_log (for detailed error logging)
  - hba_authname (for converting auth method to string name)
- Called from (representative examples):
  - CheckPasswordAuth (password authentication)
  - CheckPWChallengeAuth (password challenge authentication)
  - pg_GSS_checkauth (GSSAPI authentication)
  - pg_SSPI_recvauth (SSPI authentication)
  - ident_inet (Ident authentication)
  - auth_peer (Peer authentication)
  - CheckPAMAuth (PAM authentication)
  - CheckBSDAuth (BSD authentication)
  - CheckLDAPAuth (LDAP authentication)
  - CheckCertAuth (Certificate authentication)
  - CheckRADIUSAuth (RADIUS authentication)

## Notes and Other Information
- The function must be called exactly once per successful authentication
- The ID string is copied into TopMemoryContext to match the lifetime of MyClientConnectionInfo
- Fatal error occurs if authentication ID is set more than once, preventing authentication provider conflicts
- Connection logging (Log_connections) controls whether successful authentication events are logged
- The function should be called as soon as authentication succeeds, even if authorization might fail later
- External library-managed strings are safe to pass since they are copied to PostgreSQL's memory context