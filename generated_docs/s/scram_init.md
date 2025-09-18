# scram_init

## Location
[src/backend/libpq/auth-scram.c:236-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L236-L347)

## Overview
Initializes a new SCRAM authentication exchange status tracker and prepares the authentication state for a client connection.

## Definition


## Detailed Description
This function creates and initializes a scram_state structure to track the progress of a SCRAM authentication exchange. It validates the selected SASL mechanism, parses the stored password secret from pg_authid, and sets up the authentication context. If the user doesn't have a valid SCRAM secret or if a mock authentication is requested, the function still proceeds but marks the authentication as 'doomed' to fail later while maintaining timing-attack resistance.

The function supports two SCRAM mechanisms:
- SCRAM-SHA-256-PLUS (with channel binding, requires SSL)
- SCRAM-SHA-256 (standard variant)

When a valid secret cannot be obtained, the function creates a mock secret to prevent information leakage to attackers.

## Parameters / Member Variables
- : Connection port information containing user_name and SSL status
- : The SASL mechanism selected by the client (must be supported)
- : The role's stored secret from pg_authid.rolpassword (can be NULL for mock auth)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - strcmp
  - [get_password_type](../g/get_password_type.md)
  - [parse_scram_secret](../p/parse_scram_secret.md)
  - [mock_scram_secret](../m/mock_scram_secret.md)
  - ereport/errmsg
  - [psprintf](../p/psprintf.md)
  - SCRAM_SHA_256_PLUS_NAME
  - SCRAM_SHA_256_NAME
  - PASSWORD_TYPE_SCRAM_SHA_256
  - SCRAM_AUTH_INIT
- Called from (representative examples):
  - Referenced as function pointer in pg_be_scram_mech structure
  - Used by SASL authentication framework during client authentication

## Notes and Other Information
- Returns a void pointer to the allocated scram_state structure
- This is a static function used as a callback in the SASL mechanism interface
- Performs timing-attack resistant mock authentication when secrets are unavailable
- Validates that channel binding variants are only used with SSL connections
- Sets the 'doomed' flag when authentication should fail (invalid secrets, mock auth)
- Logs detailed error information for debugging while avoiding information leakage
- The returned state must be freed by the caller after authentication completes