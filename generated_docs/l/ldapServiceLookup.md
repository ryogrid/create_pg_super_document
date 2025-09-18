# ldapServiceLookup

## Location
src/interfaces/libpq/fe-connect.c: 5038 - 5491

## Overview
Performs an LDAP directory service lookup to retrieve PostgreSQL connection parameters from an LDAP server using a specified LDAP URL.

## Definition
```c
static int ldapServiceLookup(const char *purl, PQconninfoOption *options, PQExpBuffer errorMessage)
```

## Detailed Description
ldapServiceLookup is an internal libpq function that implements LDAP-based service discovery for PostgreSQL connections. The function parses an LDAP URL conforming to RFC 1959, connects to the specified LDAP server, performs a search query, and treats the results as PostgreSQL connection options. This allows centralized configuration management where database connection parameters can be stored in LDAP directories.

The function performs comprehensive LDAP URL validation, parsing the scheme, hostname, port, distinguished name (dn), attributes, scope, and filter components. It establishes an anonymous LDAP connection with timeout handling, executes the search query, and processes the results as key-value pairs that are added to the connection options array. The implementation handles both Windows (using ldap_connect) and OpenLDAP (using timeout options) platforms differently for connection establishment.

## Parameters / Member Variables
- `purl`: The LDAP URL string to parse and query, must conform to RFC 1959 format: ldap://host:port/dn?attributes?scope?filter?extensions
- `options`: Array of PQconninfoOption structures where the retrieved connection parameters will be stored
- `errorMessage`: PQExpBuffer for accumulating error messages during the lookup process

## Dependencies
- Functions called/Symbols referenced:
  - libpq_append_error
  - pg_strncasecmp
  - ldap_init, ldap_search_st, ldap_unbind (LDAP library functions)
  - malloc, strdup, strchr (standard C library functions)
  - ld_is_sp_tab, ld_is_nl_cr (libpq helper macros)
  - LDAP_DEF_PORT, PGLDAP_TIMEOUT, DefaultHost (constants)
- Called from (representative examples):
  - parseServiceFile

## Notes and Other Information
- Returns 0 on success, 1 for search failure, 2 for connection failure, 3 for fatal errors
- Supports both Windows and OpenLDAP implementations with platform-specific connection handling
- Requires exactly one attribute in the LDAP URL and expects exactly one search result entry
- Parses connection options from LDAP results using a state machine for quoted/unquoted values
- Performs anonymous LDAP bind with configurable timeout (PGLDAP_TIMEOUT seconds)
- The function is static (internal to libpq) and used for service file parsing
- Error messages are appended to errorMessage for return codes 1 and 3
- Memory management includes proper cleanup of LDAP structures and temporary allocations