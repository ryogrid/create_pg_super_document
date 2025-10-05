# get_hba_options

## Location
[src/backend/utils/adt/hbafuncs.c:52-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/hbafuncs.c#L52-L164)

## Overview
Creates a text array listing the authentication options specified in an HBA (Host-Based Authentication) line, returning NULL if no options are specified.

## Definition

```c
struct_array_builtin(options, noptions, TEXTOID);
```
## Detailed Description
The  function processes an HBA configuration line and extracts all authentication-related options into a PostgreSQL text array. It handles various authentication methods including GSS/SSPI, LDAP, RADIUS, and certificate-based authentication. The function examines the authentication method type and conditionally includes relevant options such as Kerberos realms, LDAP server configurations, RADIUS settings, and client certificate requirements. Each option is formatted as a "key=value" string and added to the array.

## Parameters / Member Variables
- : Pointer to an HbaLine structure containing parsed HBA configuration data including authentication method, connection parameters, and method-specific options

## Dependencies
- Functions called/Symbols referenced:
  - CStringGetTextDatum
  - [psprintf](../p/psprintf.md)
  - [construct_array_builtin](../c/construct_array_builtin.md)
  - Assert
- Types referenced:
  - [HbaLine](../H/HbaLine.md)
  - MAX_HBA_OPTIONS
  - uaGSS, uaSSPI, uaLDAP, uaRADIUS (authentication method enums)
  - clientCertOff, clientCertCA (client certificate enums)
- Called from:
  - [fill_hba_line](../f/fill_hba_line.md)

## Notes and Other Information
- The function is static and only used within the hbafuncs.c file
- Maximum number of options is limited by MAX_HBA_OPTIONS constant
- Different authentication methods expose different sets of configuration options
- Returns NULL when no options are present, otherwise returns a text array
- Handles sensitive information like LDAP bind passwords and RADIUS secrets
- Uses PostgreSQL's array construction utilities for creating the return value

## Simplified Source

```c
static ArrayType *
get_hba_options(HbaLine *hba)
{
    int noptions = 0;
    Datum options[MAX_HBA_OPTIONS];

    // GSS/SSPI authentication options
    if (hba->auth_method == uaGSS || hba->auth_method == uaSSPI) {
        if (hba->include_realm)
            options[noptions++] = CStringGetTextDatum("include_realm=true");
        if (hba->krb_realm)
            options[noptions++] = CStringGetTextDatum(psprintf("krb_realm=%s", hba->krb_realm));
    }

    // User mapping option
    if (hba->usermap)
        options[noptions++] = CStringGetTextDatum(psprintf("map=%s", hba->usermap));

    // Client certificate option
    if (hba->clientcert != clientCertOff)
        options[noptions++] = CStringGetTextDatum(psprintf("clientcert=%s",
            (hba->clientcert == clientCertCA) ? "verify-ca" : "verify-full"));

    // PAM service option
    if (hba->pamservice)
        options[noptions++] = CStringGetTextDatum(psprintf("pamservice=%s", hba->pamservice));

    // LDAP authentication options
    if (hba->auth_method == uaLDAP) {
        if (hba->ldapserver)
            options[noptions++] = CStringGetTextDatum(psprintf("ldapserver=%s", hba->ldapserver));
        if (hba->ldapport)
            options[noptions++] = CStringGetTextDatum(psprintf("ldapport=%d", hba->ldapport));
        // ... other LDAP options follow similar pattern
    }

    // RADIUS authentication options
    if (hba->auth_method == uaRADIUS) {
        if (hba->radiusservers_s)
            options[noptions++] = CStringGetTextDatum(psprintf("radiusservers=%s", hba->radiusservers_s));
        // ... other RADIUS options follow similar pattern
    }

    // Return array if options exist, NULL otherwise
    if (noptions > 0)
        return construct_array_builtin(options, noptions, TEXTOID);
    else
        return NULL;
}
```