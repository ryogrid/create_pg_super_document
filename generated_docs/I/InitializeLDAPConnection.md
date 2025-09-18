# InitializeLDAPConnection

## Location
src/backend/libpq/auth.c: 2219 - 2390

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
  - initStringInfo (initializes PostgreSQL string buffer)
  - appendStringInfoString (appends strings to buffer)
  - appendBinaryStringInfo (appends binary data to buffer)
  - errdetail_for_ldap (provides detailed LDAP error information)
- Called from (representative examples):
  - CheckLDAPAuth (referenced in auth.c:2489)

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