# CheckLDAPAuth

## Location
[src/backend/libpq/auth.c:2438-2664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2438-L2664)

## Overview
Performs LDAP authentication by establishing an LDAP connection, optionally searching for the user's DN, and attempting to bind with the provided credentials.

## Definition


## Detailed Description
The `CheckLDAPAuth` function implements PostgreSQL's LDAP authentication mechanism. It supports two primary authentication modes:

1. **Simple bind**: Uses `ldapprefix` and `ldapsuffix` to construct the bind DN directly from the username
2. **Search+bind**: Uses `ldapbasedn` to first search for the user's DN, then binds with that DN

The function performs several key operations:
- Validates LDAP server configuration
- Requests and receives the user's password
- Establishes an LDAP connection using `InitializeLDAPConnection`
- For search+bind mode: performs an initial bind with bind credentials, searches for the user, and validates the search results
- Performs the final authentication bind with the user's credentials
- Sets the authenticated identity upon successful authentication

The function includes comprehensive error handling and security measures, including input validation to prevent LDAP injection attacks by disallowing special characters in usernames.

## Parameters / Member Variables
- `port`: Pointer to the Port structure containing connection information and HBA (Host-Based Authentication) configuration including LDAP settings such as server, basedn, bind credentials, search filters, etc.

## Dependencies
- Functions called/Symbols referenced:
  - [sendAuthRequest](../s/sendAuthRequest.md): Request password from client
  - [recv_password_packet](../r/recv_password_packet.md): Receive password from client
  - [InitializeLDAPConnection](../I/InitializeLDAPConnection.md): Establish LDAP connection
  - [FormatSearchFilter](../F/FormatSearchFilter.md): Format LDAP search filter with username
  - [errdetail_for_ldap](../e/errdetail_for_ldap.md): Generate LDAP error details
  - [set_authn_id](../s/set_authn_id.md): Set authenticated identity
  - `ldap_simple_bind_s`: LDAP simple bind operation
  - `ldap_search_s`: LDAP search operation
  - `ldap_count_entries`, `ldap_first_entry`, `ldap_get_dn`: LDAP result processing
  - `ldap_unbind`: Close LDAP connection
  - [psprintf](../p/psprintf.md), `pstrdup`, `pfree`: PostgreSQL string utilities
- Called from (representative examples):
  - Authentication dispatch logic in auth.c at line 625

## Notes and Other Information
- Returns `STATUS_OK` on successful authentication, `STATUS_ERROR` on failure, or `STATUS_EOF` if client doesn't send password
- Supports both LDAP and LDAPS (secure LDAP) protocols, defaulting to appropriate ports (389 for LDAP, 636 for LDAPS)
- Implements security measures against LDAP injection by validating usernames for prohibited characters: `*`, `(`, `)`, `\\`, `/`
- For search+bind mode, supports custom search filters via `ldapsearchfilter`, attribute-based filters via `ldapsearchattribute`, or defaults to `uid` attribute
- Uses the `ldap_password_hook` for processing bind passwords, enabling password transformation if needed
- Comprehensive error reporting includes LDAP-specific error details for troubleshooting
- Memory management ensures proper cleanup of allocated strings and LDAP resources
- Supports DNS SRV record lookups for OpenLDAP when server hostname is empty but basedn is provided