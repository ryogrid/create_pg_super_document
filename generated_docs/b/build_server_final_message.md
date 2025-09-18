# build_server_final_message

## Location
[src/backend/libpq/auth-scram.c:1399-1457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L1399-L1457)

## Overview
Builds the final server-side message in a SCRAM authentication exchange, containing the server signature that proves the server's knowledge of the client's credentials.

## Definition


## Detailed Description
This function constructs the server-final-message as the concluding step of the SCRAM authentication protocol (RFC 5802). The message contains a server signature that serves as mutual authentication proof - it demonstrates that the server has successfully verified the client's credentials and possesses the correct authentication keys.

The server signature is calculated using HMAC with the following inputs:
1. **client_first_message_bare**: The client's initial message without GS2 header
2. **server_first_message**: The server's first message (containing nonce, salt, iterations)
3. **client_final_message_without_proof**: The client's final message excluding the proof portion

The signature is computed as: HMAC(ServerKey, AuthMessage), where AuthMessage is the concatenation of the above three messages separated by commas. This signature is then base64-encoded and formatted according to RFC 5802 as "v=<base64-signature>".

## Parameters / Member Variables
- : Pointer to scram_state structure containing:
  - : The server's authentication key derived during SCRAM key derivation
  - : Length of the authentication keys
  - : Hash algorithm type for HMAC computation
  - : Client's initial message for signature calculation
  - : Server's first message for signature calculation  
  - : Client's final message for signature calculation

## Dependencies
- Functions called/Symbols referenced:
  - : Create HMAC context for specified hash algorithm
  - : Initialize HMAC with key
  - : Add data to HMAC calculation
  - : Finalize HMAC and get result
  - : Free HMAC context resources
  - : Get HMAC error message
  - : Calculate base64 encoding length
  - : Encode binary data to base64
  - : Allocate memory in current memory context
  - : Format string with automatic memory allocation
  - : Maximum key length constant
- Called from (representative examples):
  - : Main SCRAM authentication exchange handler

## Notes and Other Information
- The server signature provides mutual authentication - both client and server prove knowledge of credentials
- Error handling includes validation of all HMAC operations and base64 encoding
- The returned string follows RFC 5802 format: "v=<base64-encoded-signature>"
- Memory is allocated in the current memory context and should be managed by the caller
- The function is static and only used within the auth-scram.c module
- Successful completion of this function indicates that SCRAM authentication has succeeded
- The signature calculation uses the same hash algorithm (SHA-1 or SHA-256) as specified in the SCRAM variant