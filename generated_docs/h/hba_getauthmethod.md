# hba_getauthmethod

## Location
[src/backend/libpq/hba.c:3048-3060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L3048-L3060)

## Overview
Determines the appropriate authentication method for a client connection by searching through the parsed HBA (Host-Based Authentication) configuration and storing the result in the port structure.

## Definition


## Detailed Description
The `hba_getauthmethod` function serves as a simple wrapper around the `check_hba()` function, providing the main entry point for determining authentication requirements for client connections. This function is called during the authentication process to match the client's connection parameters (database, user, source address, connection type) against the rules defined in the HBA configuration file (typically pg_hba.conf).

The function takes a `hbaPort` structure (which is actually a typedef for `struct Port`) containing all the relevant connection information and calls `check_hba()` to perform the actual lookup. The `check_hba()` function iterates through the parsed HBA rules, checking connection type, SSL/GSS encryption status, IP addresses, database names, and user roles until it finds a matching entry.

If a matching HBA entry is found, the corresponding `HbaLine` structure is stored in `port->hba` containing the authentication method and any associated parameters. If no matching entry is found, the authentication method is set to `uaImplicitReject`, which will cause the connection to be denied.

## Parameters / Member Variables
- `port`: Pointer to an hbaPort structure (typedef of struct Port) containing connection details including:
  - `database_name`: Target database name
  - `user_name`: Requested PostgreSQL username  
  - `raddr`: Remote client address information
  - `ssl_in_use`: SSL connection status
  - `gss`: GSS encryption information
  - `hba`: Output field where the matching HbaLine will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [check_hba](../c/check_hba.md) (performs the actual HBA rule matching logic)
  - [hbaPort](hbaPort.md) (typedef for struct Port containing connection information)
- Called from (representative examples):
  - [ClientAuthentication](../C/ClientAuthentication.md) (main authentication entry point in auth.c)

## Notes and Other Information
- This function is located at src/backend/libpq/hba.c:3048-3060
- The function is a thin wrapper that provides a clean API for HBA authentication lookup
- The actual matching logic is implemented in the static `check_hba()` function
- The `hbaPort` parameter is modified in-place, with the `hba` field set to point to the matching authentication rule
- This function assumes that the HBA configuration has already been loaded and parsed by `load_hba()`
- The function always succeeds in the sense that it doesn't return an error code - if no rule matches, it sets an implicit rejection rule
- The result of this function determines which specific authentication method (password, certificate, Kerberos, etc.) will be used for the connection