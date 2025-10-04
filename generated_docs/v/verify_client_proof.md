# verify_client_proof

## Location
[src/backend/libpq/auth-scram.c:1135-1188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L1135-L1188)

## Overview
A cryptographic verification function that validates the client's proof in SCRAM authentication by computing and comparing the client's key derivation against the stored authentication data.

## Definition

```c
static bool
verify_client_proof(scram_state *state)
```
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
  - [scram_H](../s/scram_H.md) (at Line 1174)
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

## Simplified Source

```c
static bool
verify_client_proof(scram_state *state)
{
    uint8 ClientSignature[SCRAM_MAX_KEY_LEN];
    uint8 ClientKey[SCRAM_MAX_KEY_LEN];
    uint8 client_StoredKey[SCRAM_MAX_KEY_LEN];
    pg_hmac_ctx *ctx = pg_hmac_create(state->hash_type);
    int i;

    // Calculate ClientSignature using HMAC with stored key and auth messages
    if (pg_hmac_init(ctx, state->StoredKey, state->key_length) < 0 ||
        pg_hmac_update(ctx, (uint8 *) state->client_first_message_bare,
                       strlen(state->client_first_message_bare)) < 0 ||
        pg_hmac_update(ctx, (uint8 *) ",", 1) < 0 ||
        pg_hmac_update(ctx, (uint8 *) state->server_first_message,
                       strlen(state->server_first_message)) < 0 ||
        pg_hmac_update(ctx, (uint8 *) ",", 1) < 0 ||
        pg_hmac_update(ctx, (uint8 *) state->client_final_message_without_proof,
                       strlen(state->client_final_message_without_proof)) < 0 ||
        pg_hmac_final(ctx, ClientSignature, state->key_length) < 0)
    {
        elog(ERROR, "could not calculate client signature: %s",
             pg_hmac_error(ctx));
    }

    pg_hmac_free(ctx);

    // Extract ClientKey by XORing proof with signature
    for (i = 0; i < state->key_length; i++)
        ClientKey[i] = state->ClientProof[i] ^ ClientSignature[i];

    // Hash ClientKey and compare with stored key
    if (scram_H(ClientKey, state->hash_type, state->key_length,
                client_StoredKey, &errstr) < 0)
        elog(ERROR, "could not hash stored key: %s", errstr);

    // Verify authentication by comparing hashed keys
    return (memcmp(client_StoredKey, state->StoredKey, state->key_length) == 0);
}
```