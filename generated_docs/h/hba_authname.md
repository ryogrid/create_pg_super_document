# hba_authname

## Location
src/backend/libpq/hba.c: 3061 - 3064

## Overview
Returns the human-readable string name corresponding to a UserAuth enumeration value, used for logging and display purposes in PostgreSQL authentication.

## Definition


## Detailed Description
The `hba_authname` function is a simple lookup utility that converts UserAuth enumeration values into their corresponding human-readable string representations. It serves as the standard interface for obtaining authentication method names throughout PostgreSQL's authentication system.

The function performs a direct array lookup using the `UserAuthName` static array, which contains string constants for each authentication method defined in the `UserAuth` enum. The array and enum are kept in sync through a compile-time assertion (`StaticAssertDecl`) that ensures the array length matches the number of enum values.

The returned string is statically allocated and should not be freed by the caller. This design ensures consistent string representations across the codebase and eliminates memory management concerns for callers.

## Parameters / Member Variables
- `auth_method`: A UserAuth enumeration value representing the authentication method (e.g., uaTrust, uaMD5, uaSCRAM, uaGSS, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - UserAuthName (static string array containing authentication method names)
  - UserAuth (enumeration type defining all supported authentication methods)
- Called from (representative examples):
  - ParallelWorkerMain (for logging worker authentication details)
  - set_authn_id (for setting authentication identifier)
  - HOSTNAME_LOOKUP_DETAIL (for detailed hostname lookup logging)
  - fill_hba_line (for populating HBA function results)
  - InitPostgres (during database initialization and authentication logging)

## Notes and Other Information
- This function is located at src/backend/libpq/hba.c:3061-3064
- The UserAuthName array is defined at src/backend/libpq/hba.c:101-118 and contains the following mappings:
  - uaReject → "reject"
  - uaImplicitReject → "implicit reject"
  - uaTrust → "trust"
  - uaIdent → "ident"
  - uaPassword → "password"
  - uaMD5 → "md5"
  - uaSCRAM → "scram-sha-256"
  - uaGSS → "gss"
  - uaSSPI → "sspi"
  - uaPAM → "pam"
  - uaBSD → "bsd"
  - uaLDAP → "ldap"
  - uaCert → "cert"
  - uaRADIUS → "radius"
  - uaPeer → "peer"
- The function includes no bounds checking, relying on callers to provide valid UserAuth values
- A compile-time assertion ensures the UserAuthName array stays synchronized with the UserAuth enum
- This function is primarily used for logging, error messages, and administrative interfaces where authentication method names need to be displayed in a human-readable format