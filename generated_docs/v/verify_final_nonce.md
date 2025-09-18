# verify_final_nonce

## Location
src/backend/libpq/auth-scram.c: 1113 - 1134

## Overview
A validation function that verifies the client's final nonce in SCRAM authentication by ensuring it correctly concatenates the original client nonce with the server nonce.

## Definition


## Detailed Description
The `verify_final_nonce` function implements a critical security check in the SCRAM authentication protocol by validating that the client has correctly constructed the final nonce. According to SCRAM specification, the final nonce should be the concatenation of the client's original nonce (sent in the client-first-message) and the server's nonce (sent in the server-first-message).

This verification ensures that both parties have correctly processed the nonce exchange, which is essential for the cryptographic integrity of the SCRAM authentication process. The function performs three specific checks: length validation to ensure the concatenated length matches, and two memory comparisons to verify that both the client and server nonce portions are correctly positioned and unchanged.

A successful verification confirms that the client has the correct nonce values and strengthens the overall security of the authentication handshake.

## Parameters / Member Variables
- `state`: Pointer to the scram_state structure containing the authentication session data, including client_nonce, server_nonce, and client_final_nonce fields

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - memcmp (standard C library function)
  - scram_state (at Line 1113)
- Called from (representative examples):
  - [scram_exchange](../s/scram_exchange.md) (at src/backend/libpq/auth-scram.c:413)
  - scram_state (at src/backend/libpq/auth-scram.c:178)

## Notes and Other Information
- Returns true if the final nonce is valid, false otherwise
- Critical for SCRAM protocol security - prevents nonce manipulation attacks
- Validates that client_final_nonce == client_nonce + server_nonce
- Uses exact byte-level comparison to prevent timing attacks
- Should be called during the client-final-message processing phase
- Failure of this check typically indicates a protocol error or potential attack attempt