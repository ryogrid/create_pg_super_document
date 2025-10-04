# CheckLDAPAuth

## Location
[src/backend/libpq/auth.c:2438-2664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2438-L2664)

## Overview
Performs LDAP authentication by establishing an LDAP connection, optionally searching for the user's DN, and attempting to bind with the provided credentials.

## Definition

```c
static int
CheckLDAPAuth(Port *port)
```
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

## Simplified Source

```c
static int
CheckLDAPAuth(Port *port)
{
    char       *passwd;
    LDAP       *ldap;
    int         r;
    char       *fulluser;
    const char *server_name;

    // Validate LDAP server configuration
    if ((!port->hba->ldapserver || port->hba->ldapserver[0] == '\0') &&
        (!port->hba->ldapbasedn || port->hba->ldapbasedn[0] == '\0'))
    {
        ereport(LOG, (errmsg("LDAP server not specified, and no ldapbasedn")));
        return STATUS_ERROR;
    }

    server_name = port->hba->ldapserver ? port->hba->ldapserver : "";

    // Set default LDAP port based on scheme
    if (port->hba->ldapport == 0)
    {
        if (port->hba->ldapscheme != NULL && strcmp(port->hba->ldapscheme, "ldaps") == 0)
            port->hba->ldapport = LDAPS_PORT;
        else
            port->hba->ldapport = LDAP_PORT;
    }

    // Request password from client
    sendAuthRequest(port, AUTH_REQ_PASSWORD, NULL, 0);
    passwd = recv_password_packet(port);
    if (passwd == NULL)
        return STATUS_EOF;

    // Initialize LDAP connection
    if (InitializeLDAPConnection(port, &ldap) == STATUS_ERROR)
    {
        pfree(passwd);
        return STATUS_ERROR;
    }

    if (port->hba->ldapbasedn)
    {
        // Search+bind mode: Find user's DN first
        char *filter;
        LDAPMessage *search_message;
        LDAPMessage *entry;
        char *attributes[] = {LDAP_NO_ATTRS, NULL};
        char *dn;
        int count;

        // Validate username for LDAP injection prevention
        for (char *c = port->user_name; *c; c++)
        {
            if (*c == '*' || *c == '(' || *c == ')' || *c == '\\' || *c == '/')
            {
                ereport(LOG, (errmsg("invalid character in user name for LDAP authentication")));
                ldap_unbind(ldap);
                pfree(passwd);
                return STATUS_ERROR;
            }
        }

        // Bind with search credentials
        r = ldap_simple_bind_s(ldap,
                               port->hba->ldapbinddn ? port->hba->ldapbinddn : "",
                               port->hba->ldapbindpasswd ? ldap_password_hook(port->hba->ldapbindpasswd) : "");
        if (r != LDAP_SUCCESS)
        {
            ereport(LOG, (errmsg("could not perform initial LDAP bind: %s", ldap_err2string(r))));
            ldap_unbind(ldap);
            pfree(passwd);
            return STATUS_ERROR;
        }

        // Build search filter
        if (port->hba->ldapsearchfilter)
            filter = FormatSearchFilter(port->hba->ldapsearchfilter, port->user_name);
        else if (port->hba->ldapsearchattribute)
            filter = psprintf("(%s=%s)", port->hba->ldapsearchattribute, port->user_name);
        else
            filter = psprintf("(uid=%s)", port->user_name);

        // Search for user
        r = ldap_search_s(ldap, port->hba->ldapbasedn, port->hba->ldapscope,
                          filter, attributes, 0, &search_message);

        if (r != LDAP_SUCCESS)
        {
            ereport(LOG, (errmsg("LDAP search failed: %s", ldap_err2string(r))));
            ldap_unbind(ldap);
            pfree(passwd);
            pfree(filter);
            return STATUS_ERROR;
        }

        // Validate search results - must find exactly one user
        count = ldap_count_entries(ldap, search_message);
        if (count != 1)
        {
            if (count == 0)
                ereport(LOG, (errmsg("LDAP user \"%s\" does not exist", port->user_name)));
            else
                ereport(LOG, (errmsg("LDAP user \"%s\" is not unique", port->user_name)));

            ldap_unbind(ldap);
            pfree(passwd);
            pfree(filter);
            ldap_msgfree(search_message);
            return STATUS_ERROR;
        }

        // Extract DN from search result
        entry = ldap_first_entry(ldap, search_message);
        dn = ldap_get_dn(ldap, entry);
        if (dn == NULL)
        {
            ereport(LOG, (errmsg("could not get dn for matching entry")));
            ldap_unbind(ldap);
            pfree(passwd);
            pfree(filter);
            ldap_msgfree(search_message);
            return STATUS_ERROR;
        }
        fulluser = pstrdup(dn);

        // Cleanup search resources
        pfree(filter);
        ldap_memfree(dn);
        ldap_msgfree(search_message);
    }
    else
    {
        // Simple bind mode: construct DN from prefix + username + suffix
        fulluser = psprintf("%s%s%s",
                            port->hba->ldapprefix ? port->hba->ldapprefix : "",
                            port->user_name,
                            port->hba->ldapsuffix ? port->hba->ldapsuffix : "");
    }

    // Authenticate user with their password
    r = ldap_simple_bind_s(ldap, fulluser, passwd);

    if (r != LDAP_SUCCESS)
    {
        ereport(LOG, (errmsg("LDAP login failed for user \"%s\": %s",
                             fulluser, ldap_err2string(r))));
        ldap_unbind(ldap);
        pfree(passwd);
        pfree(fulluser);
        return STATUS_ERROR;
    }

    // Set authenticated identity and cleanup
    set_authn_id(port, fulluser);
    ldap_unbind(ldap);
    pfree(passwd);
    pfree(fulluser);

    return STATUS_OK;
}
```