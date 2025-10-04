# set_authn_id

## Location
[src/backend/libpq/auth.c:352-389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L352-L389)

## Overview
Sets the authenticated identity for the current user connection and stores it alongside the authentication method in the client connection info structure.

## Definition

```c
enumber));
```
## Detailed Description
The  function is responsible for recording the authenticated identity of a user once authentication has been successfully completed. This function stores the authentication identifier in the global  structure and associates it with the authentication method used.

The function implements important security safeguards by ensuring that the authentication ID can only be set once per connection. If multiple authentication providers attempt to set the ID, the function treats this as a fatal error to prevent authentication conflicts.

The authenticated identity and method are logged if connection logging is enabled, providing administrators with audit trail information about successful authentications.

## Parameters / Member Variables
- : Pointer to the Port structure containing connection and HBA information
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): String containing the authenticated identity (will be copied to TopMemoryContext)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (for copying the ID string to permanent memory)
  - ereport (for logging and error reporting)
  - [errdetail_log](../e/errdetail_log.md) (for detailed error logging)
  - [hba_authname](../h/hba_authname.md) (for converting auth method to string name)
- Called from (representative examples):
  - [CheckPasswordAuth](../C/CheckPasswordAuth.md) (password authentication)
  - [CheckPWChallengeAuth](../C/CheckPWChallengeAuth.md) (password challenge authentication)
  - [pg_GSS_checkauth](../p/pg_GSS_checkauth.md) (GSSAPI authentication)
  - [pg_SSPI_recvauth](../p/pg_SSPI_recvauth.md) (SSPI authentication)
  - [ident_inet](../i/ident_inet.md) (Ident authentication)
  - [auth_peer](../a/auth_peer.md) (Peer authentication)
  - [CheckPAMAuth](../C/CheckPAMAuth.md) (PAM authentication)
  - [CheckBSDAuth](../C/CheckBSDAuth.md) (BSD authentication)
  - [CheckLDAPAuth](../C/CheckLDAPAuth.md) (LDAP authentication)
  - [CheckCertAuth](../C/CheckCertAuth.md) (Certificate authentication)
  - [CheckRADIUSAuth](../C/CheckRADIUSAuth.md) (RADIUS authentication)

## Notes and Other Information
- The function must be called exactly once per successful authentication
- The ID string is copied into TopMemoryContext to match the lifetime of MyClientConnectionInfo
- Fatal error occurs if authentication ID is set more than once, preventing authentication provider conflicts
- Connection logging (Log_connections) controls whether successful authentication events are logged
- The function should be called as soon as authentication succeeds, even if authorization might fail later
- External library-managed strings are safe to pass since they are copied to PostgreSQL's memory context

## Simplified Source

```c
static void
set_authn_id(Port *port, const char *id)
{
    Assert(id);

    // Prevent multiple authentication ID settings
    if (MyClientConnectionInfo.authn_id) {
        ereport(FATAL,
                (errmsg("authentication identifier set more than once"),
                 errdetail_log("previous identifier: \"%s\"; new identifier: \"%s\"",
                               MyClientConnectionInfo.authn_id, id)));
    }

    // Store authenticated identity and method
    MyClientConnectionInfo.authn_id = MemoryContextStrdup(TopMemoryContext, id);
    MyClientConnectionInfo.auth_method = port->hba->auth_method;

    // Log successful authentication if enabled
    if (Log_connections) {
        ereport(LOG,
                (errmsg("connection authenticated: identity=\"%s\" method=%s (%s:%d)",
                        MyClientConnectionInfo.authn_id,
                        hba_authname(MyClientConnectionInfo.auth_method),
                        port->hba->sourcefile, port->hba->linenumber)));
    }
}
```