# calculate_client_proof

## Location
src/interfaces/libpq/fe-auth-scram.c: 761 - 829

## Overview
Computes the cryptographic client proof for SCRAM authentication, demonstrating client knowledge of the password without transmitting it directly.

## Definition
static bool calculate_client_proof(fe_scram_state *state, const char *client_final_message_without_proof, uint8 *result, const char **errstr)

## Detailed Description
This function performs the core cryptographic computation of the SCRAM protocol by calculating the client proof. It derives the SaltedPassword using PBKDF2, then computes ClientKey and StoredKey according to the SCRAM specification. The function creates an HMAC signature over the authentication string (composed of client-first-message-bare, server-first-message, and client-final-message-without-proof), then XORs the ClientKey with this signature to produce the client proof. This proof allows the server to verify the client knows the password without the password itself being transmitted.

## Parameters / Member Variables
- : Pointer to fe_scram_state containing authentication data, password, salt, and iteration count
- : The client final message content excluding the proof portion
- : Output buffer to store the calculated client proof bytes
- : Pointer to error string pointer for detailed error reporting

## Dependencies
- Functions called/Symbols referenced:
  - scram_SaltedPassword (derives password using PBKDF2)
  - scram_ClientKey (derives client key from salted password)
  - scram_H (computes hash function for stored key)
  - [pg_hmac_create](../p/pg_hmac_create.md)/init/update/final/free (HMAC operations)
  - [pg_hmac_error](../p/pg_hmac_error.md) (error reporting for HMAC operations)
- Called from:
  - [build_client_final_message](../b/build_client_final_message.md) (constructs final client authentication message)

## Notes and Other Information
- Implements the mathematical core of SCRAM authentication protocol (RFC 5802)
- Uses PBKDF2 for password-based key derivation with configurable iteration count
- HMAC computation covers the entire authentication conversation to prevent tampering
- ClientProof = ClientKey XOR ClientSignature provides zero-knowledge proof of password
- SaltedPassword is stored in state for later server signature verification
- Critical security function - any errors in implementation could compromise authentication
- Returns false on any cryptographic operation failure with detailed error message
- The result buffer must be at least state->key_length bytes