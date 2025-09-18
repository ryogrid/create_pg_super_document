# verify_client_proof

## Location
src/backend/libpq/auth-scram.c: 1135 - 1188

## Overview
A cryptographic verification function that validates the client's proof in SCRAM authentication by computing and comparing the client's key derivation against the stored authentication data.

## Definition


## Detailed Description
The `verify_client_proof` function implements the core cryptographic verification logic for SCRAM authentication. It performs the final validation step in the SCRAM protocol by verifying that the client possesses the correct password without the server needing to store the password in plaintext.

The function follows the SCRAM protocol's mathematical proof verification process:
1. Calculates the ClientSignature using HMAC with the stored key and the complete authentication message exchange
2. Extracts the ClientKey by XORing the received ClientProof with the computed ClientSignature  
3. Hashes the extracted ClientKey to produce a candidate StoredKey
4. Compares the candidate StoredKey with the actual StoredKey from the database

This cryptographic approach ensures that only a client knowing the correct password can generate a valid proof, while the server never needs to handle the plaintext password during verification.

## Parameters / Member Variables
- `state`: Pointer to the scram_state structure containing all authentication session data, including cryptographic keys, message fragments, and protocol parameters

## Dependencies
- Functions called/Symbols referenced:
  - [pg_hmac_create](../p/pg_hmac_create.md) (at Line 1140)
  - [pg_hmac_init](../p/pg_hmac_init.md) (at Line 1149) 
  - [pg_hmac_update](../p/pg_hmac_update.md) (at Line 1150, 1153, 1154, 1157, 1158)
  - [pg_hmac_final](../p/pg_hmac_final.md) (at Line 1161)
  - [pg_hmac_error](../p/pg_hmac_error.md) (at Line 1164)
  - [pg_hmac_free](../p/pg_hmac_free.md) (at Line 1167)
  - scram_H (at Line 1174)
  - memcmp (standard C library function)
  - elog (PostgreSQL error logging)
- Called from (representative examples):
  - [scram_exchange](../s/scram_exchange.md) (at src/backend/libpq/auth-scram.c:438)
  - scram_state (at src/backend/libpq/auth-scram.c:177)

## Notes and Other Information
- Returns true if the client proof is valid, false otherwise
- Implements the SCRAM proof verification algorithm as specified in RFC 5802
- Uses PostgreSQL's HMAC implementation for cryptographic operations
- Supports multiple hash algorithms through the hash_type parameter
- Critical for password verification without plaintext password storage
- ClientProof = ClientKey XOR ClientSignature, so ClientKey = ClientProof XOR ClientSignature
- StoredKey = H(ClientKey), where H is the hash function
- Includes comprehensive error handling for cryptographic operation failures
- Designed to work with mock authentication scenarios for timing attack prevention