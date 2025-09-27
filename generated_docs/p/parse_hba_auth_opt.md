# parse_hba_auth_opt

## Location
[src/backend/libpq/hba.c:2049-2468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2049-L2468)

## Overview
Parses individual authentication options from pg_hba.conf configuration lines into HbaLine structures, validating option syntax and compatibility with the specified authentication method.

## Definition

```c
enumber;
```
## Detailed Description
This function processes name-value pairs representing authentication options found in pg_hba.conf entries. It validates that each option is appropriate for the specified authentication method and stores the parsed values in the corresponding fields of the HbaLine structure. The function performs comprehensive validation including:

- Option compatibility checking with authentication methods
- Value format validation (e.g., port numbers, boolean values)
- SSL/TLS-specific option validation for hostssl connections
- LDAP URL parsing and validation when LDAP support is available
- Network address resolution for RADIUS server configurations

The function is designed to provide detailed error reporting with file context information to help administrators identify and correct configuration issues.

## Parameters / Member Variables
- : The name of the authentication option to parse
- : The value associated with the authentication option 
- : Pointer to HbaLine structure where parsed option will be stored
- : Error reporting level for ereport() calls
- : Output parameter for error message string (set on failure)

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](pstrdup.md) (string duplication)
  - ereport/errcode/errmsg/errcontext (error reporting)
  - INVALID_AUTH_OPTION/REQUIRE_AUTH_OPTION (validation macros)
  - ldap_url_parse/ldap_free_urldesc (LDAP URL handling)
  - [SplitGUCList](../S/SplitGUCList.md) (comma-separated list parsing)
  - [pg_getaddrinfo_all](pg_getaddrinfo_all.md)/pg_freeaddrinfo_all (network address resolution)
  - Authentication method constants (uaPAM, uaLDAP, uaRADIUS, etc.)
  - Client certificate constants (clientCertFull, clientCertCA, etc.)
- Called from:
  - [parse_hba_line](parse_hba_line.md) (src/backend/libpq/hba.c:1875)

## Notes and Other Information
- Supports a comprehensive set of authentication options including PAM, LDAP, RADIUS, Kerberos/GSSAPI, SSPI, and SSL client certificates
- LDAP URL parsing is only available when compiled with OpenLDAP support
- The function maintains backwards compatibility while providing detailed validation
- Error messages include file name and line number context for easier troubleshooting
- Returns true on successful parsing, false on error with detailed error message provided
- Many options are specific to certain authentication methods and will be rejected if used inappropriately

## Simplified Source

```c
// Simplified version of parse_hba_auth_opt
static bool
parse_hba_auth_opt(char *name, char *val, HbaLine *hbaline,
                   int elevel, char **err_msg)
{
    int line_num = hbaline->linenumber;
    char *file_name = hbaline->sourcefile;

#ifdef USE_LDAP
    hbaline->ldapscope = LDAP_SCOPE_SUBTREE;
#endif

    // Core logic: Parse authentication options based on name
    if (strcmp(name, "map") == 0) {
        // Validate map option is used with compatible auth methods
        if (hbaline->auth_method != uaIdent &&
            hbaline->auth_method != uaPeer &&
            hbaline->auth_method != uaGSS &&
            hbaline->auth_method != uaSSPI &&
            hbaline->auth_method != uaCert)
            return validation_error("map", "ident, peer, gssapi, sspi, and cert");
        hbaline->usermap = pstrdup(val);
    }
    else if (strcmp(name, "clientcert") == 0) {
        // Validate SSL connection requirement
        if (hbaline->conntype != ctHostSSL)
            return validation_error("clientcert can only be configured for hostssl rows");

        // Parse client certificate verification level
        if (strcmp(val, "verify-full") == 0) {
            hbaline->clientcert = clientCertFull;
        }
        else if (strcmp(val, "verify-ca") == 0) {
            // Additional validation for cert auth method
            if (hbaline->auth_method == uaCert)
                return validation_error("clientcert only accepts verify-full when using cert authentication");
            hbaline->clientcert = clientCertCA;
        }
        else {
            return validation_error("invalid value for clientcert");
        }
    }
    else if (strcmp(name, "clientname") == 0) {
        // Validate SSL connection and parse client name format
        if (hbaline->conntype != ctHostSSL)
            return validation_error("clientname can only be configured for hostssl rows");

        if (strcmp(val, "CN") == 0)
            hbaline->clientcertname = clientCertCN;
        else if (strcmp(val, "DN") == 0)
            hbaline->clientcertname = clientCertDN;
        else
            return validation_error("invalid value for clientname");
    }

    // PAM authentication options
    else if (strcmp(name, "pamservice") == 0) {
        validate_auth_method(uaPAM, "pamservice", "pam");
        hbaline->pamservice = pstrdup(val);
    }
    else if (strcmp(name, "pam_use_hostname") == 0) {
        validate_auth_method(uaPAM, "pam_use_hostname", "pam");
        hbaline->pam_use_hostname = (strcmp(val, "1") == 0);
    }

    // LDAP authentication options
    else if (strcmp(name, "ldapurl") == 0) {
        validate_auth_method(uaLDAP, "ldapurl", "ldap");
        parse_ldap_url(val, hbaline, elevel, err_msg);
    }
    else if (strcmp(name, "ldaptls") == 0) {
        validate_auth_method(uaLDAP, "ldaptls", "ldap");
        hbaline->ldaptls = (strcmp(val, "1") == 0);
    }
    else if (is_ldap_option(name)) {
        // Handle other LDAP options: ldapscheme, ldapserver, ldapport, etc.
        validate_auth_method(uaLDAP, name, "ldap");
        store_ldap_option(name, val, hbaline);
    }

    // Kerberos/GSSAPI/SSPI options
    else if (strcmp(name, "krb_realm") == 0) {
        validate_auth_method_any(uaGSS, uaSSPI, "krb_realm", "gssapi and sspi");
        hbaline->krb_realm = pstrdup(val);
    }
    else if (strcmp(name, "include_realm") == 0) {
        validate_auth_method_any(uaGSS, uaSSPI, "include_realm", "gssapi and sspi");
        hbaline->include_realm = (strcmp(val, "1") == 0);
    }
    else if (is_sspi_option(name)) {
        // Handle SSPI-specific options: compat_realm, upn_username
        validate_auth_method(uaSSPI, name, "sspi");
        store_sspi_option(name, val, hbaline);
    }

    // RADIUS authentication options
    else if (is_radius_option(name)) {
        validate_auth_method(uaRADIUS, name, "radius");
        parse_radius_option(name, val, hbaline, elevel, err_msg);
    }

    // Unrecognized option
    else {
        return validation_error("unrecognized authentication option name");
    }

    return true;
}
```

Key simplifications made:
- Consolidated repetitive option parsing into helper function calls
- Abstracted detailed LDAP URL parsing into separate function
- Grouped similar options (LDAP, SSPI, RADIUS) with helper functions
- Simplified boolean value parsing with direct comparison
- Reduced detailed error reporting to focus on core validation logic
- Removed platform-specific conditional compilation details
- Consolidated validation macros into helper function calls
- Focused on the main execution path rather than exhaustive error handling