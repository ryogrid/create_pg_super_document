# build_client_final_message

## Location
src/interfaces/libpq/fe-auth-scram.c: 450 - 601

## Overview
Constructs the final SCRAM authentication message sent by the client, including channel binding data and the cryptographic client proof to verify client knowledge of the password.

## Definition
static char *build_client_final_message(fe_scram_state *state)

## Detailed Description
This function builds the client-final-message in the SCRAM authentication exchange, which is the second and final message sent by the client to the server. It constructs the message in two parts: first the client-final-message-without-proof (containing channel binding and nonce information), then appends the client proof calculated using the calculate_client_proof function. The function handles channel binding data differently depending on whether SCRAM-SHA-256-PLUS is being used, which requires SSL certificate hash information for enhanced security.

## Parameters / Member Variables
- : Pointer to fe_scram_state structure containing authentication state, connection info, nonce data, and cryptographic parameters

## Dependencies
- Functions called/Symbols referenced:
  - calculate_client_proof (computes cryptographic proof)
  - pgtls_get_peer_certificate_hash (gets SSL cert hash for channel binding)
  - pg_b64_encode (base64 encoding for binary data)
  - initPQExpBuffer/appendPQExpBuffer* (message construction)
  - enlargePQExpBuffer (buffer management)
  - libpq_append_conn_error (error reporting)
- Called from:
  - scram_exchange (main SCRAM authentication handler)

## Notes and Other Information
- Handles two SCRAM variants: SCRAM-SHA-256 and SCRAM-SHA-256-PLUS
- For SCRAM-SHA-256-PLUS, incorporates SSL certificate hash as channel binding data
- Channel binding flag must be consistent with build_client_first_message
- Stores client-final-message-without-proof separately for server verification later
- The client proof demonstrates knowledge of the password without transmitting it
- Critical security function - proper channel binding prevents man-in-the-middle attacks
- Returns dynamically allocated memory that must be freed by caller