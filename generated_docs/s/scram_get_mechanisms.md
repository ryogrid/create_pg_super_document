# scram_get_mechanisms

## Location
[src/backend/libpq/auth-scram.c:202-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L202-L235)

## Overview
Returns a list of SASL mechanisms supported by the SCRAM authentication module for PostgreSQL backend authentication.

## Definition


## Detailed Description
This function builds a list of supported SCRAM authentication mechanisms and appends them to a StringInfo buffer. The mechanisms are listed in decreasing order of importance, with channel-binding variants (which require SSL) listed first when available. The mechanism names are separated by null bytes ('\0') for convenience in building the FE/BE packet that lists the available authentication mechanisms to the client.

The function supports:
- SCRAM-SHA-256-PLUS (channel binding variant, only when SSL is in use)  
- SCRAM-SHA-256 (standard variant)

## Parameters / Member Variables
- : Connection port information containing SSL status and other connection details
- : StringInfo buffer where mechanism names will be appended, separated by null bytes

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString
  - appendStringInfoChar
  - SCRAM_SHA_256_PLUS_NAME
  - SCRAM_SHA_256_NAME
  - USE_SSL (preprocessor macro)
- Called from (representative examples):
  - Referenced as function pointer in pg_be_scram_mech structure
  - Used by SASL authentication framework

## Notes and Other Information
- This is a static function used as a callback in the SASL mechanism structure
- Channel binding variants are only advertised when SSL is in use
- The function is part of the pg_be_sasl_mech interface for SCRAM authentication
- Mechanism names are defined as constants (SCRAM_SHA_256_NAME, SCRAM_SHA_256_PLUS_NAME)
- The ordering prioritizes more secure mechanisms (with channel binding) first