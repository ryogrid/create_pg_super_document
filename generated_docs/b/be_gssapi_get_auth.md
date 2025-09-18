# be_gssapi_get_auth

## Location
[src/backend/libpq/be-secure-gssapi.c:741-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-gssapi.c#L741-L752)

## Overview
Returns whether GSSAPI authentication was successfully used on the given connection.

## Definition


## Detailed Description
The  function is a simple query function that checks whether GSSAPI authentication was completed for a specific connection. It performs null safety checks on the port and its GSSAPI state structure before returning the authentication status.

This function is typically used by other parts of PostgreSQL to determine the authentication method used for logging, statistics, or security policy decisions. It only indicates that GSSAPI authentication occurred, not whether GSSAPI encryption is active (which would be checked separately).

## Parameters / Member Variables
- : Pointer to Port structure containing connection state information

## Dependencies
- Functions called/Symbols referenced:
  - None (simple accessor function)
- Struct members accessed:
  - : GSSAPI state structure
  - : Boolean flag indicating if GSSAPI authentication was used
- Called from:
  - : For collecting connection statistics
  - : During authentication process validation

## Notes and Other Information
- Returns  if the port is NULL or has no GSSAPI state structure
- Returns  only if GSSAPI authentication was successfully completed
- This function is separate from encryption status - a connection can have GSSAPI authentication without GSSAPI encryption
- Used primarily for auditing and monitoring purposes to track authentication methods
- The  flag is set during the GSSAPI authentication process in other functions