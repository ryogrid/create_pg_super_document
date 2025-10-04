# InitializeLDAPConnection

## Location
[src/backend/libpq/auth.c:2219-2390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2219-L2390)

## Overview
The InitializeLDAPConnection function establishes and configures an LDAP connection for PostgreSQL authentication, including support for TLS encryption and automatic LDAP server discovery via DNS SRV records.

## Definition
```c
static int InitializeLDAPConnection(Port *port, LDAP **ldap)
```

## Detailed Description
This function handles the complex process of establishing an LDAP connection with multiple configuration options and fallback mechanisms. It supports both ldap:// and ldaps:// schemes, with platform-specific implementations for Windows (using ldap_sslinit) and Unix-like systems. On systems with OpenLDAP, it provides advanced features like automatic LDAP server discovery through DNS SRV record lookups when no explicit server is configured.

The function constructs proper LDAP URIs from the configuration, sets the LDAP protocol version to LDAPv3, and optionally initiates TLS sessions for secure communication. It includes comprehensive error handling and logging for each step of the connection process.

## Parameters / Member Variables
- `port`: Pointer to Port structure containing HBA (Host-Based Authentication) configuration with LDAP settings including server, port, scheme, TLS options, and base DN
- `ldap`: Double pointer to LDAP structure that will be populated with the initialized LDAP connection handle

## Dependencies
- Functions called/Symbols referenced:
  - ldap_sslinit (Windows-specific SSL LDAP initialization)
  - ldap_init (standard LDAP initialization)
  - ldap_initialize (OpenLDAP extension for URI-based initialization)
  - ldap_dn2domain (extracts domain name from LDAP DN for SRV lookup)
  - ldap_domain2hostlist (discovers LDAP servers via DNS SRV records)
  - ldap_set_option (configures LDAP connection options)
  - ldap_start_tls_s (initiates TLS session)
  - ldap_unbind (cleans up LDAP connection on errors)
  - ldap_err2string (converts LDAP error codes to strings)
  - ldap_memfree (frees OpenLDAP-allocated memory)
  - [initStringInfo](../i/initStringInfo.md) (initializes PostgreSQL string buffer)
  - [appendStringInfoString](../a/appendStringInfoString.md) (appends strings to buffer)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (appends binary data to buffer)
  - [errdetail_for_ldap](../e/errdetail_for_ldap.md) (provides detailed LDAP error information)
- Called from (representative examples):
  - [CheckLDAPAuth](../C/CheckLDAPAuth.md) (referenced in auth.c:2489)

## Notes and Other Information
- Supports automatic LDAP server discovery via DNS SRV records when ldapserver is not explicitly configured
- Handles multiple LDAP servers by constructing space-separated URI lists for failover
- Platform-specific implementations: Windows uses ldap_sslinit for SSL, Unix uses ldap_initialize with URI schemes
- Automatically sets LDAP protocol version to LDAPv3 for compatibility and security
- Supports both StartTLS (ldaptls option) and native SSL (ldaps scheme) for secure connections
- Includes fallback to basic ldap_init on systems without OpenLDAP's ldap_initialize extension
- Comprehensive error handling with detailed logging for troubleshooting connection issues
- Properly manages memory allocation and cleanup for OpenLDAP-specific functions
- Returns STATUS_OK on successful connection establishment, STATUS_ERROR on any failure
- Performs DNS domain extraction from LDAP base DN for automatic server discovery (format: ou=blah,dc=foo,dc=bar becomes foo.bar)
- The ldaps scheme is only supported on platforms with appropriate SSL/TLS LDAP library support

## Simplified Source

```c
// Simplified version of InitializeLDAPConnection
static int InitializeLDAPConnection(Port *port, LDAP **ldap) {
    const char *scheme = port->hba->ldapscheme ? port->hba->ldapscheme : "ldap";
    int ldapversion = LDAP_VERSION3;
    int r;

#ifdef WIN32
    // Step 1: Windows-specific LDAP initialization
    if (strcmp(scheme, "ldaps") == 0) {
        *ldap = ldap_sslinit(port->hba->ldapserver, port->hba->ldapport, 1);
    } else {
        *ldap = ldap_init(port->hba->ldapserver, port->hba->ldapport);
    }

    if (!*ldap) {
        ereport(LOG, (errmsg("could not initialize LDAP: error code %d",
                            (int) LdapGetLastError())));
        return STATUS_ERROR;
    }
#else
    // Step 2: Unix/Linux LDAP initialization
#ifdef HAVE_LDAP_INITIALIZE
    // Build URI list for OpenLDAP
    StringInfoData uris;
    initStringInfo(&uris);

    char *hostlist = NULL;
    char *p;

    // Auto-discover LDAP servers via DNS SRV if no server specified
    if (!port->hba->ldapserver || port->hba->ldapserver[0] == '\0') {
        char *domain;
        if (ldap_dn2domain(port->hba->ldapbasedn, &domain) ||
            ldap_domain2hostlist(domain, &hostlist)) {
            ereport(LOG, (errmsg("LDAP authentication could not find DNS SRV records"),
                         errhint("Set an LDAP server name explicitly.")));
            ldap_memfree(domain);
            return STATUS_ERROR;
        }
        ldap_memfree(domain);
        p = hostlist;
    } else {
        p = port->hba->ldapserver;
    }

    // Build space-separated URI list
    do {
        size_t size = strcspn(p, " ");
        if (uris.len > 0) appendStringInfoChar(&uris, ' ');

        appendStringInfo(&uris, "%s://%.*s", scheme, (int)size, p);
        if (!hostlist) {  // Add port for explicit servers
            appendStringInfo(&uris, ":%d", port->hba->ldapport);
        }

        p += size;
        while (*p == ' ') ++p;
    } while (*p);

    if (hostlist) ldap_memfree(hostlist);

    // Initialize LDAP connection with URI
    r = ldap_initialize(ldap, uris.data);
    pfree(uris.data);

    if (r != LDAP_SUCCESS) {
        ereport(LOG, (errmsg("could not initialize LDAP: %s", ldap_err2string(r))));
        return STATUS_ERROR;
    }
#else
    // Fallback for systems without ldap_initialize
    if (strcmp(scheme, "ldaps") == 0) {
        ereport(LOG, (errmsg("ldaps not supported with this LDAP library")));
        return STATUS_ERROR;
    }

    *ldap = ldap_init(port->hba->ldapserver, port->hba->ldapport);
    if (!*ldap) {
        ereport(LOG, (errmsg("could not initialize LDAP: %m")));
        return STATUS_ERROR;
    }
#endif
#endif

    // Step 3: Set LDAP protocol version
    r = ldap_set_option(*ldap, LDAP_OPT_PROTOCOL_VERSION, &ldapversion);
    if (r != LDAP_SUCCESS) {
        ereport(LOG, (errmsg("could not set LDAP protocol version: %s",
                            ldap_err2string(r))));
        ldap_unbind(*ldap);
        return STATUS_ERROR;
    }

    // Step 4: Start TLS if requested
    if (port->hba->ldaptls) {
#ifndef WIN32
        r = ldap_start_tls_s(*ldap, NULL, NULL);
#else
        r = ldap_start_tls_s(*ldap, NULL, NULL, NULL, NULL);
#endif
        if (r != LDAP_SUCCESS) {
            ereport(LOG, (errmsg("could not start LDAP TLS session: %s",
                                ldap_err2string(r))));
            ldap_unbind(*ldap);
            return STATUS_ERROR;
        }
    }

    return STATUS_OK;
}
```