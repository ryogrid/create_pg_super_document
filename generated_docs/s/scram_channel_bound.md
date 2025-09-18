# scram_channel_bound

## Location
src/interfaces/libpq/fe-auth-scram.c: 154 - 177

## Overview
Determines whether channel binding was successfully employed during a completed SCRAM authentication exchange on the client side.

## Definition


## Detailed Description
This function verifies that a SCRAM authentication exchange not only completed successfully but also used channel binding for enhanced security. Channel binding provides additional protection against man-in-the-middle attacks by cryptographically binding the authentication to the underlying secure transport layer (typically TLS). The function performs several validation checks: ensuring the exchange state exists, confirming the exchange completed (FE_SCRAM_FINISHED state), and verifying that the SCRAM-SHA-256-PLUS mechanism (which includes channel binding) was actually used rather than the regular SCRAM-SHA-256 variant.

## Parameters / Member Variables
- : Opaque pointer to the fe_scram_state structure containing the client-side SCRAM authentication state

## Dependencies
- Functions called/Symbols referenced:
  - fe_scram_state (structure)
  - FE_SCRAM_FINISHED (state constant)
  - SCRAM_SHA_256_PLUS_NAME (mechanism name constant)
- Called from (representative examples):
  - No direct references found (likely called through function pointer or API)

## Notes and Other Information
- This is a client-side function (located in fe-auth-scram.c) as opposed to server-side authentication code
- The function is declared static, indicating it's only used within the fe-auth-scram.c file  
- Should only be called after a successful SCRAM exchange to determine authentication strength
- Returns false for any incomplete, failed, or non-channel-bound authentication attempts
- Channel binding provides mutual authentication - the server authenticates itself to the client
- Located in src/interfaces/libpq/fe-auth-scram.c:154-177