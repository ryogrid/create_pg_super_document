# get_hba_options

## Location
src/backend/utils/adt/hbafuncs.c: 52 - 164

## Overview
Creates a text array listing the authentication options specified in an HBA (Host-Based Authentication) line, returning NULL if no options are specified.

## Definition


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