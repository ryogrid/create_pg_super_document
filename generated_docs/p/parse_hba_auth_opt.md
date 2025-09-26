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