# ldapServiceLookup

## Location
[src/interfaces/libpq/fe-connect.c:5038-5491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L5038-L5491)

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
  - [libpq_append_error](libpq_append_error.md)
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - ldap_init, ldap_search_st, ldap_unbind (LDAP library functions)
  - malloc, strdup, strchr (standard C library functions)
  - ld_is_sp_tab, ld_is_nl_cr (libpq helper macros)
  - LDAP_DEF_PORT, PGLDAP_TIMEOUT, DefaultHost (constants)
- Called from (representative examples):
  - [parseServiceFile](../p/parseServiceFile.md)

## Notes and Other Information
- Returns 0 on success, 1 for search failure, 2 for connection failure, 3 for fatal errors
- Supports both Windows and OpenLDAP implementations with platform-specific connection handling
- Requires exactly one attribute in the LDAP URL and expects exactly one search result entry
- Parses connection options from LDAP results using a state machine for quoted/unquoted values
- Performs anonymous LDAP bind with configurable timeout (PGLDAP_TIMEOUT seconds)
- The function is static (internal to libpq) and used for service file parsing
- Error messages are appended to errorMessage for return codes 1 and 3
- Memory management includes proper cleanup of LDAP structures and temporary allocations

## Simplified Source

```c
static int ldapServiceLookup(const char *purl, PQconninfoOption *options, PQExpBuffer errorMessage) {
    // Parse LDAP URL components (scheme, hostname, port, dn, attributes, scope, filter)
    char *url = strdup(purl);
    if (!url) {
        libpq_append_error(errorMessage, "out of memory");
        return 3;
    }

    // Validate URL scheme and extract components
    if (pg_strncasecmp(url, LDAP_URL, strlen(LDAP_URL)) != 0) {
        libpq_append_error(errorMessage, "invalid LDAP URL: scheme must be ldap://");
        free(url);
        return 3;
    }

    // Extract hostname, dn, attributes, scope, filter, and optional port
    char *hostname = url + strlen(LDAP_URL);
    char *dn, *attrs[2] = {NULL, NULL}, *scopestr, *filter;
    int port = LDAP_DEF_PORT, scope;

    // Parse URL components (detailed parsing logic simplified)
    // Set scope based on scopestr: "base", "one", or "sub"

    // Initialize LDAP connection
    LDAP *ld = ldap_init(hostname, port);
    if (!ld) {
        libpq_append_error(errorMessage, "could not create LDAP structure");
        free(url);
        return 3;
    }

    // Perform anonymous bind with timeout handling
    LDAP_TIMEVAL time = {PGLDAP_TIMEOUT, 0};
#ifdef WIN32
    if (ldap_connect(ld, &time) != LDAP_SUCCESS) {
        free(url);
        ldap_unbind(ld);
        return 2;  // Connection timeout
    }
#else
    // OpenLDAP: set timeout and perform simple bind
    ldap_set_option(ld, LDAP_OPT_NETWORK_TIMEOUT, &time);
    // Perform bind and wait for result
#endif

    // Execute LDAP search
    LDAPMessage *res = NULL;
    int rc = ldap_search_st(ld, dn, scope, filter, attrs, 0, &time, &res);
    if (rc != LDAP_SUCCESS) {
        libpq_append_error(errorMessage, "lookup on LDAP server failed: %s", ldap_err2string(rc));
        if (res) ldap_msgfree(res);
        ldap_unbind(ld);
        free(url);
        return 1;
    }

    // Validate exactly one result entry
    if (ldap_count_entries(ld, res) != 1) {
        libpq_append_error(errorMessage, "expected exactly one LDAP entry");
        ldap_msgfree(res);
        ldap_unbind(ld);
        free(url);
        return 1;
    }

    // Get attribute values and concatenate them
    LDAPMessage *entry = ldap_first_entry(ld, res);
    struct berval **values = ldap_get_values_len(ld, entry, attrs[0]);

    // Concatenate all values into single string with newlines
    char *result = malloc(/* calculated size */);
    // Copy values to result string

    // Parse result as connection options using state machine
    // Parse key=value pairs, handle quoted values, update options array

    // Cleanup
    free(result);
    ldap_value_free_len(values);
    ldap_msgfree(res);
    ldap_unbind(ld);
    free(url);

    return 0;  // Success
}
```