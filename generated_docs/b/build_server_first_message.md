# build_server_first_message

## Location
[src/backend/libpq/auth-scram.c:1189-1252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L1189-L1252)

## Overview
Builds the first server-side message sent to the client in a SCRAM authentication exchange, containing the server nonce, salt, and iteration count according to RFC 5802.

## Definition

```c
static char *
build_server_first_message(scram_state *state)
```
## Detailed Description
This function constructs the server-first-message as part of the SCRAM (Salted Challenge Response Authentication Mechanism) protocol implementation. The message follows RFC 5802 syntax and contains:

1. **Nonce**: A combination of client nonce (received earlier) and a newly generated server nonce
2. **Salt**: Base64-encoded salt value stored in the authentication state
3. **Iteration count**: Number of PBKDF2 iterations to be performed

The server nonce is generated using cryptographically secure random bytes (SCRAM_RAW_NONCE_LEN bytes) which are then base64-encoded. The complete message format is: 

This message is critical for the SCRAM authentication flow as it provides the client with the necessary parameters to compute the authentication proof.

## Parameters / Member Variables
- : Pointer to scram_state structure containing:
  - : The nonce received from the client's first message
  - : Base64-encoded salt for password hashing
  - : Number of PBKDF2 iterations
  - : Generated server nonce (populated by this function)
  - : The complete first message (populated by this function)

## Dependencies
- Functions called/Symbols referenced:
  - : Generate cryptographically secure random bytes
  - : Calculate required buffer length for base64 encoding
  - : Encode raw bytes to base64 format
  - : Allocate memory in current memory context
  - : Format string with automatic memory allocation
  - : Duplicate string in current memory context
  - : Constant defining raw nonce length
- Called from (representative examples):
  - : Main SCRAM authentication exchange handler

## Notes and Other Information
- The function generates a server nonce using secure random number generation to prevent replay attacks
- Error handling includes checks for random number generation failure and base64 encoding failure
- The returned string is allocated in the current memory context and should be freed by the caller
- The server nonce is stored in the scram_state for use in subsequent authentication steps
- Follows RFC 5802 specification for SCRAM server-first-message format
- The function is static and only used within the auth-scram.c module